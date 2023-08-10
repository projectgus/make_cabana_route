use chrono::{DateTime, Utc};
use clap::Parser;
use itertools::Itertools;
use make_route::log_capnp::sentinel::SentinelType;
use make_route::qlog::{read_can_messages, CANMessage, LogInput, QlogWriter};
use make_route::video::{SegmentVideoEncoder, SourceVideo};
use make_route::Nanos;
use merging_iterator::MergeIter;
use serde::Deserialize;
use serde_yaml;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

// Duration of a route segment
const SEGMENT_NANOS: Nanos = Duration::from_secs(60).as_nanos() as Nanos;

// Each CAN event can span up to this long (effectively, giving all those messages the same timestamp)
const CAN_EVENT_TIME: Nanos = Duration::from_millis(10).as_nanos() as Nanos;

// Insert a thumbnail at these intervals
const THUMBNAIL_INTERVAL: Nanos = Duration::from_millis(2500).as_nanos() as Nanos;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path of YAML file with route information
    #[arg(short, long, default_value = "routes.yml")]
    yaml_path: PathBuf,

    /// Path to generate Cabana data directory
    #[arg(short, long, default_value = "data_dir")]
    data_dir: PathBuf,
}

#[derive(Deserialize, PartialEq, Debug)]
struct LogInfo {
    car: String,
    car_details: String,
    logfile: String, // Path?
    video: String,
    sync: LogSyncInfo,
}

#[derive(Deserialize, PartialEq, Debug)]
struct LogSyncInfo {
    video_s: f64,
    log_us: i64,
}

impl LogSyncInfo {
    /* Give the nanosecond offset to subtract from a CAN log timestamp, to
    convert it into the pts timestamp of the video.
    */
    fn can_ts_offs(&self) -> Nanos {
        // The video timestamp that corresponds to log timestamp
        let video_us = (self.video_s * 1000_000.0) as i64;
        // The log timestamp that corresponds to video timestamp 0:00
        let log_us_at_zero = self.log_us - video_us;

        return log_us_at_zero * 1000;
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    ffmpeg::init().unwrap();

    let args = Args::parse();

    let yaml_abs = args.yaml_path.canonicalize()?;
    let log_dir = yaml_abs.parent().unwrap();

    let f = std::fs::File::open(args.yaml_path)?;
    let logs: Vec<LogInfo> = serde_yaml::from_reader(f)?;

    for log_info in logs {
        let log_path = log_dir.join(log_info.logfile);
        let video_path = log_dir.join(log_info.video);

        /* See replay Route::parseRoute() in cabana for the regex that loads the route name.
         *
         * There is an optional 16 character hex prefix field with the dongle ID.
         * Currently leave this off, it looks like Cabana may be happy without it.
         */

        // TODO: base this off an entry in the YAML file not file metadata
        let metadata = fs::metadata(&log_path)?;
        let created: DateTime<Utc> = metadata.created()?.into();
        let route_name = created.format("%Y-%m-%d--%H-%M-%S").to_string();
        let mut route_dir_base = args.data_dir.to_path_buf();
        route_dir_base.push(route_name);

        process_log(
            &log_path,
            &video_path,
            route_dir_base.as_os_str(),
            log_info.sync.can_ts_offs(),
        )?;
    }

    Ok(())
}

fn process_log(
    log_path: &Path,
    video_path: &Path,
    route_dir_base: &OsStr,
    can_ts_offs: Nanos,
) -> Result<(), Box<dyn Error>> {
    // Read CAN messages, and sort them by timestamp
    // (not guaranteed from the CSV log, if there are CAN messages from >1 bus)
    let can_inputs = read_can_messages(log_path, can_ts_offs)?
        .into_iter()
        .map(|m| LogInput::CAN(m))
        .sorted();

    eprintln!("Opening video {video_path:?}...");

    let mut source_video = SourceVideo::new(&video_path)?;

    let properties = source_video.properties();

    let frame_inputs = source_video.video_frames().map(|f| LogInput::Frame(f));

    eprintln!("Preparing source data...");

    // Merge the CAN and Frame inputs, keeping them sorted as we read them
    let inputs = MergeIter::new(can_inputs, frame_inputs);

    // Sort the inputs and group them into segments
    let segments = inputs
        .peekable()
        .group_by(|input| input.timestamp() / SEGMENT_NANOS);

    for (segment_idx, inputs) in &segments {
        let mut inputs = inputs.peekable();
        let mut segment_dir = OsString::from(route_dir_base);
        segment_dir.push(format!("--{}", segment_idx));

        let mut frame_id = 0;

        let segment_dir = PathBuf::from(segment_dir);

        eprintln!("Writing segment {segment_idx} to {segment_dir:?}...");

        std::fs::create_dir_all(&segment_dir)?;

        let mut qlog = QlogWriter::new(segment_dir.join("qlog.bz2"))?;
        let seg_video_path = segment_dir.join("qcamera.ts");

        // Only encode new segment videos if they don't already exist, as this is the slowest
        // and most CPU intensive part
        let mut segment_video = match seg_video_path.exists() {
            true => {
                eprintln!("Skipping existing {seg_video_path:?}");
                None
            }
            _ => Some(SegmentVideoEncoder::new(&seg_video_path, &properties)?),
        };

        let first_ts = match inputs.peek() {
            Some(first) => first.timestamp(),
            None => 0,
        };

        qlog.write_init_data(first_ts);

        if segment_idx == 0 {
            qlog.write_car_params(first_ts);
            qlog.write_sentinel(first_ts, SentinelType::StartOfRoute);
        }
        qlog.write_sentinel(first_ts, SentinelType::StartOfSegment);

        let mut last_thumbnail: Nanos = 0;

        let mut can_msgs: Vec<CANMessage> = vec![];

        for input in inputs {
            if !can_msgs.is_empty() && input.timestamp() - can_msgs[0].timestamp() > CAN_EVENT_TIME
            {
                // Flush the current set of CAN messages to an event
                // in qlog whenever CAN_EVENT_LEN time has passed
                qlog.write_can(&can_msgs);
                can_msgs.clear();
            }

            match input {
                LogInput::CAN(can_msg) => {
                    can_msgs.push(can_msg);
                }
                LogInput::Frame(ref frame) => {
                    let ts = input.timestamp();

                    if let Some(ref mut encode) = segment_video {
                        encode.send_frame(&frame)?;
                    }

                    qlog.write_frame_encode_idx(ts, segment_idx as i32, frame_id);
                    if ts - last_thumbnail > THUMBNAIL_INTERVAL {
                        let jpeg = frame.encode_jpeg()?;
                        qlog.write_thumbnail(ts, ts + THUMBNAIL_INTERVAL, frame_id, &jpeg);
                        last_thumbnail = ts;
                    }

                    frame_id += 1;
                }
            }
        }

        // Flush any final batch of CAN messages
        qlog.write_can(&can_msgs);

        if let Some(encode) = segment_video {
            encode.finish();

            if frame_id == 0 {
                // No frames actually got written for this segment, so get rid of the
                // zero byte video file (otherwise Openpilot complains)
                println!("Warning: empty video segment. CAN log probably runs longer than video");
                std::fs::remove_file(seg_video_path)?;
            }
        }

        qlog.write_sentinel(0, SentinelType::EndOfSegment);
    }

    Ok(())
}

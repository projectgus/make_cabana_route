use chrono::{DateTime, Local};
use clap::Parser;
use itertools::Itertools;
use make_route::input::{read_can_messages, CANMessage, LogInput};
use make_route::log_capnp::sentinel::SentinelType;
use make_route::qlog::QlogWriter;
use make_route::video::{SegmentVideoEncoder, SourceVideo};
use make_route::Nanos;
use merging_iterator::MergeIter;
use serde::Deserialize;
use std::error::Error;
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
    route_timestamp: Option<DateTime<Local>>,
    logfile: PathBuf,
    video: Option<PathBuf>,
    sync: LogSyncInfo,
}

impl LogInfo {
    // Convert relative paths to absolute ones, return an error if paths don't exist
    fn canonicalise_paths(&mut self, relative_to: &Path) -> Result<(), Box<dyn Error>> {
        let relative_to = relative_to
            .canonicalize()?
            .parent()
            .expect("relative_to file should always have a parent directory.")
            .to_path_buf();
        self.logfile = relative_to.join(&self.logfile);

        // Check logfile exists
        self.logfile.metadata()?;

        if let Some(video) = &self.video {
            let video = relative_to.join(video);
            // Check video exists
            video.metadata()?;
            self.video = Some(video);
        }

        Ok(())
    }

    // Routes are identified in openpilot by their timestamp.
    //
    // If route_timestamp is set in the YAML file, use this. Otherwise,
    // use the modification date of the video file or the log file..
    fn route_timestamp(&self) -> DateTime<Local> {
        if let Some(ts) = self.route_timestamp {
            ts
        } else if let Some(video) = &self.video {
            video
                .metadata()
                .expect("video file should already exist")
                .modified()
                .unwrap()
                .into()
        } else {
            self.logfile
                .metadata()
                .expect("logfile checked already")
                .modified()
                .expect("logfile checked already")
                .into()
        }
    }

    // Segment directories in the data directory are based on the route timestamp,
    // plus a suffix for the segment number
    //
    // See replay Route::parseRoute() in openpilot for the regex that resolves the route name.
    //
    // Routes also have an optional 16 character hex suffix field with the dongle ID.
    // Currently leave this off, it looks like Cabana is happy without it.
    fn segment_dir_path(&self, data_dir: &Path, segment_idx: i64) -> PathBuf {
        let mut result = data_dir.to_path_buf();
        result.push(format!(
            "{}--{}",
            self.route_timestamp().format("%Y-%m-%d--%H-%M-%S"),
            segment_idx
        ));
        result
    }
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
        let video_us = (self.video_s * 1_000_000.0) as i64;
        // The log timestamp that corresponds to video timestamp 0:00
        let log_us_at_zero = self.log_us - video_us;

        log_us_at_zero * 1000
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    ffmpeg::init().unwrap();

    let args = Args::parse();

    let f = std::fs::File::open(&args.yaml_path)?;
    let mut logs: Vec<LogInfo> = serde_yaml::from_reader(f)?;

    // Fix up paths, this will also error out early if any files are not found
    for info in &mut logs {
        info.canonicalise_paths(&args.yaml_path)?;
    }

    for info in &logs {
        process_log(info, &args.data_dir)?;
    }

    Ok(())
}

fn process_log(info: &LogInfo, data_dir: &Path) -> Result<(), Box<dyn Error>> {
    // Read CAN messages, and sort them by timestamp
    // (not guaranteed from the CSV log, if there are CAN messages from >1 bus)
    eprintln!("Loading CAN messages {0:?}...", info.logfile);
    let can_inputs = read_can_messages(&info.logfile, info.sync.can_ts_offs())?
        .into_iter()
        .map(LogInput::CAN)
        .sorted();

    let mut source_video = None;
    let mut video_properties = None;

    if let Some(video_path) = &info.video {
        eprintln!("Opening video {video_path:?}...");
        let sv = SourceVideo::new(video_path)?;
        video_properties = Some(sv.properties());
        source_video = Some(sv);
    };

    let inputs: Box<dyn Iterator<Item = LogInput>> = match &mut source_video {
        Some(source_video) => {
            // If we have video and CAN message inputs, merge them together
            // keeping the output sorted by timestamp
            let frames = source_video.video_frames().map(LogInput::Frame);
            Box::new(MergeIter::new(can_inputs, frames))
        }
        // If only have CAN messages, can iterate them as-is
        None => Box::new(can_inputs),
    };

    // Sort the inputs and group them into segments
    let segments = inputs
        .peekable()
        .group_by(|input| input.timestamp() / SEGMENT_NANOS);

    for (segment_idx, inputs) in &segments {
        let mut inputs = inputs.peekable();

        let mut frame_id = 0;

        let segment_dir = info.segment_dir_path(data_dir, segment_idx);

        eprintln!("Writing segment {segment_idx} to {segment_dir:?}...");

        std::fs::create_dir_all(&segment_dir)?;

        let mut qlog = QlogWriter::new(segment_dir.join("qlog.bz2"))?;
        let seg_video_path = segment_dir.join("qcamera.ts");

        let mut segment_video = if let Some(properties) = &video_properties {
            if !seg_video_path.try_exists()? {
                Some(SegmentVideoEncoder::new(&seg_video_path, properties)?)
            } else {
                // Don't encode new a segment video if the it already exists, as this is the slowest
                // and most CPU intensive part
                eprintln!("Skipping existing {seg_video_path:?}");
                None
            }
        } else {
            None
        };

        let first_ts = inputs.peek().map(|f| f.timestamp()).unwrap_or(0);

        qlog.write_init_data(first_ts);

        if segment_idx == 0 {
            qlog.write_car_params(first_ts);
            qlog.write_sentinel(first_ts, SentinelType::StartOfRoute);
        }
        qlog.write_sentinel(first_ts, SentinelType::StartOfSegment);

        let mut last_thumbnail: Nanos = 0;

        let mut can_msgs: Vec<CANMessage> = vec![];

        for input in inputs {
            // Flush the current set of CAN messages to an event
            // in qlog whenever CAN_EVENT_LEN time has passed
            if !can_msgs.is_empty() && input.timestamp() - can_msgs[0].timestamp() > CAN_EVENT_TIME
            {
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
                        encode.send_frame(frame)?;
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

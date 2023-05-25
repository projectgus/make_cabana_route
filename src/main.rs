use bzip2::write::BzEncoder;
use bzip2::Compression;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log_capnp::sentinel::SentinelType;
use serde::Deserialize;
use serde_yaml;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use video::SourceFrame;

extern crate ffmpeg;

pub mod video;

// Type for nanosecond timestamps
type Nanos = i64;

// Up to 10ms of CAN messages are grouped into one CAN event
const CAN_EVENT_LEN: Nanos = 10 * 1000 * 1000;

// Duration of a route segment
const SEGMENT_NANOS: Nanos = Duration::from_secs(60).as_nanos() as Nanos;

pub mod car_capnp {
    include!(concat!(env!("OUT_DIR"), "/car_capnp.rs"));
}
pub mod legacy_capnp {
    include!(concat!(env!("OUT_DIR"), "/legacy_capnp.rs"));
}
pub mod log_capnp {
    include!(concat!(env!("OUT_DIR"), "/log_capnp.rs"));
}

pub mod rlog {
    //    use crate::log_capnp::{encode_index, event, sentinel};
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

struct RouteMeta {
    data_dir: PathBuf,
    route_name: String,
    start_ts: SystemTime,
}

impl RouteMeta {
    fn new(
        data_dir: &Path,
        route_name: &str,
        start_ts: SystemTime,
        end_ts: SystemTime,
    ) -> RouteMeta {
        RouteMeta {
            data_dir: PathBuf::from(data_dir),
            route_name: String::from(route_name),
            start_ts,
        }
    }

    /* Return the path of the the directory holding segment number 'index' */
    fn segment_dir(&self, index: u64) -> PathBuf {
        // Should range check in here?
        let dirname = format!("{}-{}", self.route_name, index);
        return PathBuf::from(dirname);
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    ffmpeg::init().unwrap();

    // TODO: make these command line args
    let yaml_path = Path::new("../../kona_logs/log_data.yml");
    let data_dir = Path::new("../../kona_logs/data_dir_new");

    let yaml_abs = yaml_path.canonicalize()?;
    let log_dir = yaml_abs.parent().unwrap();

    let f = std::fs::File::open(yaml_path)?;
    let logs: Vec<LogInfo> = serde_yaml::from_reader(f)?;

    for log_info in logs {
        let log_path = log_dir.join(log_info.logfile);
        let video_path = log_dir.join(log_info.video);

        /* See replay Route::parseRoute() in cabana for the regex that loads the route name.
         *
         * There is an optional 16 character alphanumeric prefix field with the dongle ID.
         * Currently leave this off, it looks like Cabana may be happy without it.
         *
         * Similarly, we leave off the --<segment_num> suffix for now
         *
         * TODO: base this off an entry in the YAML file not file metadata
         */
        let metadata = fs::metadata(&log_path)?;
        let created: DateTime<Utc> = metadata.created()?.into();
        let route_name = created.format("%Y-%m-%d--%H-%M-%S").to_string();
        let mut route_dir_base = data_dir.to_path_buf();
        route_dir_base.push(route_name);

        process_log(
            &log_path,
            &video_path,
            route_dir_base.as_os_str(),
            log_info.sync.can_ts_offs(),
        )?;
        break; // Stop early
    }

    Ok(())
}

// Wrapper enum for all inputs to the route log
#[derive(Eq)]
enum LogInput {
    CAN(CANMessage),
    Frame(SourceFrame),
}

impl LogInput {
    // Return timestamp in nanoseconds
    fn timestamp(&self) -> Nanos {
        match self {
            LogInput::CAN(m) => m.timestamp,
            LogInput::Frame(s) => s.ts_ns,
        }
    }
}

impl From<CANMessage> for LogInput {
    fn from(value: CANMessage) -> Self {
        LogInput::CAN(value)
    }
}

impl From<SourceFrame> for LogInput {
    fn from(value: SourceFrame) -> Self {
        LogInput::Frame(value)
    }
}

impl Ord for LogInput {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp().cmp(&other.timestamp())
    }
}

impl PartialOrd for LogInput {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LogInput {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp() == other.timestamp()
    }
}

fn process_log(
    log_path: &Path,
    video_path: &Path,
    route_dir_base: &OsStr,
    can_ts_offs: Nanos,
) -> Result<(), Box<dyn Error>> {
    eprintln!("Opening CAN log {:?}...", log_path);

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(log_path)?;

    // Merge the CAN messages and the Video frames into a vector of LogInputs that
    // we can then sort.
    //
    // Note: once BinaryHeap::drain_sorted becomes stable, can maybe use BinaryHeap
    // here.
    //
    // It's tempting to treat both iterators (CAN Messages and frames) as sorted and merge
    // them, but actually the CAN message log may not be 100% sorted if it contains captures
    // from >1 bus.
    let mut inputs: Vec<LogInput> = Vec::new();

    for can_msg in rdr.records().map(|r| match r {
        Ok(r) => CANMessage::parse_from(r, can_ts_offs),
        Err(e) => panic!("Error reading CSV file: {}", e), // TODO: error handling!
    }) {
        let can_msg = can_msg?;

        // TODO: For now dropping any CAN timestamp that comes before the video
        // started. Could conceivably adjust the start earlier instead and have empty video
        if can_msg.timestamp >= 0 {
            inputs.push(LogInput::CAN(can_msg));
        }
    }

    eprintln!("Opening video {video_path:?}...");

    let mut source_video = video::SourceVideo::new(&video_path)?;

    inputs.extend(source_video.video_frames().map(|f| LogInput::Frame(f)));

    eprintln!("Preparing source data...");

    // Sort the LogInputs and group them into segments
    let segments = inputs
        .into_iter()
        .sorted()
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

        let mut rlog = RLogWriter::new(segment_dir.join("rlog.bz2"))?;
        let seg_video_path = segment_dir.join("qcamera.ts");

        // Only encode new segment videos if they don't already exist, as this is the slowest
        // and most CPU intensive part
        let mut segment_video = match seg_video_path.exists() {
            true => {
                eprintln!("Skipping existing {seg_video_path:?}");
                None
            }
            _ => Some(video::SegmentVideoEncoder::new(
                &seg_video_path,
                &source_video,
            )?),
        };

        let first_ts = match inputs.peek() {
            Some(first) => first.timestamp(),
            None => 0,
        };

        rlog.write_init_data(first_ts);

        if segment_idx == 0 {
            rlog.write_car_params(first_ts);
            rlog.write_sentinel(first_ts, SentinelType::StartOfRoute);
        }
        rlog.write_sentinel(first_ts, SentinelType::StartOfSegment);

        let mut can_msgs: Vec<CANMessage> = vec![];

        for input in inputs {
            match input {
                LogInput::CAN(can_msg) => {
                    if !can_msgs.is_empty()
                        && can_msg.timestamp - can_msgs[0].timestamp > CAN_EVENT_LEN
                    {
                        // Flush the current set of CAN messages to an event
                        // in rlog whenever it spans more than CAN_EVENT_LEN
                        rlog.write_can(&can_msgs);
                        can_msgs.clear();
                    }
                    can_msgs.push(can_msg);
                }
                LogInput::Frame(ref frame) => {
                    if let Some(ref mut encode) = segment_video {
                        encode.send_frame(&frame)?;
                    }

                    rlog.write_frame_encode_idx(input.timestamp(), segment_idx as i32, frame_id);
                    frame_id += 1;

                    // TODO: Write a thumbnail to rlog periodically
                }
            }
        }

        // Flush any final batch of CAN messages
        rlog.write_can(&can_msgs);

        if let Some(encode) = segment_video {
            encode.finish();

            if frame_id == 0 {
                // No frames actually got written for this segment, so get rid of the
                // zero byte video file (otherwise Openpilot complains)
                println!("Warning: empty video segment. CAN log probably runs longer than video");
                std::fs::remove_file(seg_video_path)?;
            }
        }

        rlog.write_sentinel(0, SentinelType::EndOfSegment);
    }

    Ok(())
}

#[derive(Eq, PartialEq, Debug)]
struct CANMessage {
    timestamp: Nanos,
    can_id: u32,
    is_extended_id: bool,
    bus_no: u8,
    data: Vec<u8>,
}

impl CANMessage {
    // TODO: improve error propagation
    fn parse_from(record: csv::StringRecord, ts_offs: Nanos) -> Result<Self, Box<dyn Error>> {
        let mut data: Vec<u8> = vec![];
        data.reserve(8);

        // in this format, each record has a variable number of fields
        // and we want to concatenate the variable data fields
        let mut fields = record.iter();

        let ts_us: i64 = fields.next().unwrap().parse()?;
        let can_id = u32::from_str_radix(fields.next().unwrap(), 16)?;
        let is_extended_id = fields.next().unwrap() == "true";
        let bus_no = fields.next().unwrap().parse()?;

        // iterate the remaining variable number of data fields d1..d8
        // TODO: this can probably be made into .collect()
        for d in fields {
            data.push(u8::from_str_radix(d, 16)?);
        }

        Ok(CANMessage {
            timestamp: (ts_us * 1000) as Nanos - ts_offs,
            can_id,
            is_extended_id,
            bus_no,
            data,
        })
    }
}

struct RLogWriter {
    last_timestamp: Nanos,
    writer: BzEncoder<File>,
}

impl RLogWriter {
    fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let writer = BzEncoder::new(File::create(&path)?, Compression::new(6));
        Ok(Self {
            writer,
            last_timestamp: 0,
        })
    }

    fn write_event(
        self: &mut Self,
        mono_time: Nanos,
        fill_event_cb: impl Fn(log_capnp::event::Builder),
    ) {
        let mut message = ::capnp::message::Builder::new_default();
        let mut event = message.init_root::<log_capnp::event::Builder>();

        // If necessary make the timestamps monotonic
        self.last_timestamp = mono_time.max(self.last_timestamp + 1);

        event.set_valid(true);
        event.set_log_mono_time(self.last_timestamp as u64);
        fill_event_cb(event);
        capnp::serialize::write_message(&mut self.writer, &message).unwrap();
    }

    fn write_init_data(self: &mut Self, mono_time: Nanos) {
        self.write_event(mono_time, |event| {
            let mut _init_data = event.init_init_data(); // Not setting any fields here for now
        });
    }

    fn write_car_params(self: &mut Self, mono_time: Nanos) {
        self.write_event(mono_time, |event| {
            let mut car_params = event.init_car_params();
            car_params.set_car_name("TODO name");
            car_params.set_car_fingerprint("TODO fingerprint");
        });
    }

    fn write_sentinel(self: &mut Self, mono_time: Nanos, sentinel_type: SentinelType) {
        self.write_event(mono_time, |event| {
            let mut sentinel = event.init_sentinel();
            sentinel.set_type(sentinel_type);
        });
    }

    fn write_can(&mut self, can_msgs: &[CANMessage]) {
        if can_msgs.is_empty() {
            return;
        }

        self.write_event(can_msgs[0].timestamp, |event| {
            let len = can_msgs.len().try_into().unwrap();
            let mut can_evt = event.init_can(len);
            for (idx, msg) in can_msgs.iter().enumerate() {
                let mut evt_msg = can_evt.reborrow().get(idx as u32);
                evt_msg.set_address(msg.can_id as u32);
                evt_msg.set_dat(&msg.data);
                evt_msg.set_src(msg.bus_no);

                // Not sure what to do with BusTime property
                evt_msg.set_bus_time((msg.timestamp % 0xFFFF) as u16);
            }
        });
    }

    fn write_frame_encode_idx(&mut self, mono_time: Nanos, segment_num: i32, frame_id: u32) {
        self.write_event(mono_time, |event| {
            let mut encode_idx = event.init_road_encode_idx();
            encode_idx.set_frame_id(frame_id);
            encode_idx.set_type(log_capnp::encode_index::Type::FullHEVC);
            encode_idx.set_encode_id(frame_id); // Seems this can be same as Frame ID?
            encode_idx.set_segment_num(segment_num);
            encode_idx.set_segment_id(frame_id); // Appears to be the same(!)
            encode_idx.set_segment_id_encode(frame_id); // Seems ignored?
            encode_idx.set_timestamp_sof(mono_time as u64);
            encode_idx.set_timestamp_eof(mono_time as u64); // TODO: set properly
        });
    }
}

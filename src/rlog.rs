use crate::log_capnp;
use crate::log_capnp::sentinel::SentinelType;
use crate::video::SourceFrame;
use crate::Nanos;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};

// Wrapper enum for all inputs to the route log
#[derive(Eq)]
pub enum LogInput {
    CAN(CANMessage),
    Frame(SourceFrame),
}

impl LogInput {
    // Return timestamp in nanoseconds
    pub fn timestamp(&self) -> Nanos {
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

// Parser for CAN messages from CSV log
#[derive(Eq, PartialEq, Debug)]
pub struct CANMessage {
    timestamp: Nanos,
    can_id: u32,
    is_extended_id: bool,
    bus_no: u8,
    data: Vec<u8>,
}

impl CANMessage {
    // TODO: improve error propagation
    pub fn parse_from(record: csv::StringRecord, ts_offs: Nanos) -> Result<Self, Box<dyn Error>> {
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

    pub fn timestamp(self: &Self) -> Nanos {
        return self.timestamp;
    }
}

pub fn read_can_messages(
    csv_log_path: &Path,
    can_ts_offs: Nanos,
) -> Result<Vec<CANMessage>, Box<dyn Error>> {
    eprintln!("Opening CAN log {:?}...", csv_log_path);

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(csv_log_path)?;

    Ok(rdr
        .records()
        .map(|r| match r {
            Ok(r) => CANMessage::parse_from(r, can_ts_offs),
            Err(e) => panic!("Error reading CSV file: {}", e), // TODO: error handling!
        })
        .map(|m| m.unwrap()) // TODO: more error handling!
        // TODO: For now dropping any CAN timestamp that comes before the video
        // started. Could conceivably adjust the start earlier instead and have empty video
        .filter(|m| m.timestamp >= 0)
        .collect())
}

// Struct to wrap writing an rlog.bz2 file
pub struct RLogWriter {
    last_timestamp: Nanos,
    writer: BzEncoder<File>,
}

impl RLogWriter {
    pub fn new(path: PathBuf) -> Result<Self, std::io::Error> {
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

    pub fn write_init_data(self: &mut Self, mono_time: Nanos) {
        self.write_event(mono_time, |event| {
            let mut _init_data = event.init_init_data(); // Not setting any fields here for now
        });
    }

    pub fn write_car_params(self: &mut Self, mono_time: Nanos) {
        self.write_event(mono_time, |event| {
            let mut car_params = event.init_car_params();
            car_params.set_car_name("TODO name");
            car_params.set_car_fingerprint("TODO fingerprint");
        });
    }

    pub fn write_sentinel(self: &mut Self, mono_time: Nanos, sentinel_type: SentinelType) {
        self.write_event(mono_time, |event| {
            let mut sentinel = event.init_sentinel();
            sentinel.set_type(sentinel_type);
        });
    }

    pub fn write_can(&mut self, can_msgs: &[CANMessage]) {
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

    pub fn write_frame_encode_idx(&mut self, mono_time: Nanos, segment_num: i32, frame_id: u32) {
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

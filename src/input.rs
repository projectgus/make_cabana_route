use std::error::Error;
use std::path::Path;

use crate::video::SourceFrame;
use crate::Nanos;

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
    pub timestamp: Nanos,
    pub can_id: u32,
    pub is_extended_id: bool,
    pub bus_no: u8,
    pub data: Vec<u8>,
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

    pub fn timestamp(&self) -> Nanos {
        self.timestamp
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

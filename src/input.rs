use std::error::Error;
use std::path::Path;

use itertools::Itertools;
use serde::Deserialize;

use crate::video::SourceFrame;
use crate::Nanos;

// Wrapper enum for all inputs to the route log
#[derive(Eq)]
pub enum LogInput {
    CAN(CANMessage),
    Frame(SourceFrame),
    Alert(Alert),
}

impl LogInput {
    // Return timestamp in nanoseconds
    pub fn timestamp(&self) -> Nanos {
        match self {
            LogInput::CAN(m) => m.timestamp,
            LogInput::Frame(s) => s.ts_ns,
            LogInput::Alert(s) => s.timestamp,
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

impl Ord for CANMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp().cmp(&other.timestamp())
    }
}

impl PartialOrd for CANMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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
        // When the log contains >1 bus of data, the messages can be slightly out
        // of order
        .sorted()
        .collect())
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum AlertStatus {
    Normal,
    UserPrompt,
    Critical,
}

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Alert {
    pub timestamp: Nanos,
    pub status: AlertStatus,
    pub message: Option<String>,
}

// Scan the CAN messages for gaps that may indicate faults in the CAN logging
pub fn find_missing_can_messages(messages: &[CANMessage]) -> Vec<Alert> {
    let mut result = vec![];
    let mut last_timestamp = messages.first().map(|m| m.timestamp()).unwrap_or(0);
    const MISSING_THRESHOLD: Nanos = 500_000_000;

    for m in messages {
        if m.timestamp() - last_timestamp > MISSING_THRESHOLD {
            let msg = format!(
                "Possible lost CAN messages.\nGap of {:.3}s with no message",
                (m.timestamp() - last_timestamp) as f64 / 1_000_000_000.0
            );
            result.push(Alert {
                status: AlertStatus::Critical,
                message: Some(msg),
                timestamp: last_timestamp,
            });
            result.push(Alert {
                status: AlertStatus::Normal,
                message: None,
                timestamp: m.timestamp(),
            });
        }
        last_timestamp = m.timestamp();
    }
    result
}

/* Takes a list of individual alerts and expands them to cover the whole video
 * time span, with one alert each 100ms. Each alert is repeated until the next
 * alert starts (recall some alerts have message None).
 *
 * This is necessary so they display in Cabana during playback.
 */
pub fn expand_alerts(alerts: Vec<Alert>) -> Vec<LogInput> {
    let first_ts = match alerts.first() {
        Some(first) => first.timestamp,
        _ => 0,
    };
    let last_ts = match alerts.last() {
        Some(last) => last.timestamp,
        _ => first_ts,
    };

    let mut result = vec![];

    let mut ts = first_ts;

    let mut peekable = alerts.into_iter().peekable();

    while let Some(alert) = peekable.next() {
        let next_at = peekable.peek().map(|a| a.timestamp).unwrap_or(last_ts);
        while ts < next_at {
            let mut new_alert = alert.clone();
            new_alert.timestamp = ts;
            result.push(LogInput::Alert(new_alert));
            ts += 100_000_000; // 100ms
        }
    }

    result
}

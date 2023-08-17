use crate::input::CANMessage;
use crate::log_capnp;
use crate::log_capnp::sentinel::SentinelType;
use crate::Nanos;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use std::fs::File;
use std::path::PathBuf;

// Struct to wrap writing an qlog.bz2 file
pub struct QlogWriter {
    last_timestamp: Nanos,
    writer: BzEncoder<File>,
}

impl QlogWriter {
    pub fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let writer = BzEncoder::new(File::create(path)?, Compression::new(6));
        Ok(Self {
            writer,
            last_timestamp: 0,
        })
    }

    fn write_event(&mut self, mono_time: Nanos, fill_event_cb: impl Fn(log_capnp::event::Builder)) {
        let mut message = ::capnp::message::Builder::new_default();
        let mut event = message.init_root::<log_capnp::event::Builder>();

        // If necessary make the timestamps monotonic
        self.last_timestamp = mono_time.max(self.last_timestamp + 1);

        event.set_valid(true);
        event.set_log_mono_time(self.last_timestamp as u64);
        fill_event_cb(event);
        capnp::serialize::write_message(&mut self.writer, &message).unwrap();
    }

    pub fn write_init_data(&mut self, mono_time: Nanos) {
        self.write_event(mono_time, |event| {
            let mut _init_data = event.init_init_data(); // Not setting any fields here for now
        });
    }

    pub fn write_car_params(&mut self, mono_time: Nanos) {
        self.write_event(mono_time, |event| {
            let mut car_params = event.init_car_params();
            car_params.set_car_name("TODO name");
            car_params.set_car_fingerprint("TODO fingerprint");
        });
    }

    pub fn write_sentinel(&mut self, mono_time: Nanos, sentinel_type: SentinelType) {
        self.write_event(mono_time, |event| {
            let mut sentinel = event.init_sentinel();
            sentinel.set_type(sentinel_type);
        });
    }

    pub fn write_can(&mut self, can_msgs: &[CANMessage]) {
        if can_msgs.is_empty() {
            return;
        }

        self.write_event(can_msgs[0].timestamp(), |event| {
            let len = can_msgs.len().try_into().unwrap();
            let mut can_evt = event.init_can(len);
            for (idx, msg) in can_msgs.iter().enumerate() {
                let mut evt_msg = can_evt.reborrow().get(idx as u32);
                evt_msg.set_address(msg.can_id);
                evt_msg.set_dat(&msg.data);
                evt_msg.set_src(msg.bus_no);
                evt_msg.set_bus_time(0);
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

    pub fn write_thumbnail(
        &mut self,
        mono_time: Nanos,
        end_ts: Nanos,
        frame_id: u32,
        jpeg_data: &[u8],
    ) {
        self.write_event(mono_time, |event| {
            let mut thumbnail = event.init_thumbnail();
            thumbnail.set_frame_id(frame_id);
            thumbnail.set_timestamp_eof(end_ts as u64);
            thumbnail.set_thumbnail(jpeg_data);
        });
    }
}

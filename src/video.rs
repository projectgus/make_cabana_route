// Copyright (c) 2023 Angus Gratton
// SPDX-License-Identifier: GPL-2.0-or-later
use std::path::Path;
use std::rc::Rc;
use std::{cell::RefCell, error::Error};

use ffmpeg::{
    codec, decoder, encoder, format, frame, media, software::scaling, Dictionary, Packet, Rational,
};
use jpeg_encoder;

const TARGET_FPS: u32 = 20;

const JPEG_QUALITY: u8 = 80;

// TODO: consider making these runtime configurable
const JPEG_MAX_WIDTH: u32 = 640;
/// Maximum width of an embedded JPEG thumbnail
const VIDEO_MAX_WIDTH: u32 = 1280;
/// Maximum width of the output video frame

pub struct SegmentVideoEncoder {
    octx: format::context::Output,
    encoder: encoder::Video,
    video_stream_index: usize,
    frame_count: usize,
    pkt_count: usize,
}

impl SegmentVideoEncoder {
    pub fn new(
        path: &Path,
        properties: &VideoProperties,
        dump_info: bool,
    ) -> Result<Self, Box<dyn Error>> {
        let properties = properties.scale_to_width(VIDEO_MAX_WIDTH);
        let mut octx = format::output(path).unwrap();

        let mut ost = octx.add_stream()?;
        let video_stream_index = ost.index();

        let codec = encoder::find(codec::Id::HEVC).unwrap();
        let mut video = codec::Encoder::new(codec)?.video()?;

        video.set_height(properties.height);
        video.set_width(properties.width);
        video.set_format(properties.format);
        video.set_frame_rate(Some(Rational::new(TARGET_FPS as i32, 1)));
        video.set_colorspace(properties.color_space);
        video.set_color_range(properties.color_range);

        // This time base seems to be required by HEVC, but unsure how it's supposed
        // to be set
        if let Some(time_base) = properties.time_base {
            video.set_time_base(Some(time_base.invert()));
        }
        video.set_flags(codec::Flags::GLOBAL_HEADER);

        eprintln!("Writing segment video to {}...", path.display());

        let mut x264_opts = Dictionary::new();
        x264_opts.set("preset", "fast"); // default is medium. TODO: make configurable?
        x264_opts.set("crf", "40"); // default is 28. lower == higher quality, bigger files.
        let encoder = video
            .open_with(x264_opts)
            .expect("error opening HEVC encoder");
        ost.set_parameters(encoder.parameters());

        if dump_info {
            format::context::output::dump(&octx, 0, path.to_str());
        }
        octx.write_header().unwrap();

        Ok(Self {
            octx,
            encoder,
            video_stream_index,
            frame_count: 0,
            pkt_count: 0,
        })
    }

    pub fn send_frame(&mut self, frame: &SourceFrame) -> Result<(), Box<dyn Error>> {
        self.encoder.send_frame(&frame.frame)?;
        self.receive_packets()?;
        self.frame_count += 1;
        Ok(())
    }

    fn receive_packets(&mut self) -> Result<(), Box<dyn Error>> {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            self.pkt_count += 1;
            encoded.set_stream(self.video_stream_index);
            encoded.write_interleaved(&mut self.octx).unwrap();
        }

        Ok(())
    }

    pub fn finish(mut self) {
        self.encoder.send_eof().unwrap();
        self.receive_packets().unwrap();
        self.octx.write_trailer().unwrap();
    }
}

pub struct SourceVideo {
    ictx: format::context::Input,
    video_stream_index: usize,
}

// It's hard to borrow the source ffmpeg Video struct for each encoding session, as
// it requires borrowing it and it's also borrowed for the source frames.
//
// Hence, make this little wrapper struct to copy around the key properties of
// the source video and use for each segment.
#[derive(Clone, Debug)]
pub struct VideoProperties {
    height: u32,
    width: u32,
    format: format::Pixel,
    time_base: Option<Rational>,
    color_space: ffmpeg::color::Space,
    color_range: ffmpeg::color::Range,
}

impl VideoProperties {
    fn scale_to_width(&self, max_width: u32) -> VideoProperties {
        /* Return VideoProperties with the maximum width, preserving aspect ratio.
         */
        let mut res = self.clone();

        if self.width > max_width {
            res.width = max_width;
            res.height = max_width * self.height / self.width;
        }

        res
    }
}

pub struct SourceFrame {
    pub frame: frame::Video,
    pub ts_ns: i64,
    jpeg_scaler_context: Rc<RefCell<scaling::Context>>,
}

impl SourceVideo {
    pub fn new(video_file: &Path) -> Result<Self, Box<dyn Error>> {
        let ictx = format::input(video_file)?;
        let input = ictx
            .streams()
            .best(media::Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)?;
        let video_stream_index = input.index();

        Ok(Self {
            ictx,
            video_stream_index,
        })
    }

    pub fn video_decoder(&self) -> decoder::Video {
        let input = self
            .ictx
            .streams()
            .best(media::Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)
            .unwrap(); // TODO: error handling!
        input.decoder().unwrap().open().unwrap().video().unwrap() // TODO: error handling!
    }

    // Didn't have any luck implementing IntoIter for this, but this is kind of better
    // as more flexible
    pub fn video_frames(&mut self) -> SourceFrameIterator<'_> {
        let decoder = self.video_decoder();
        let output_props = self.properties().scale_to_width(JPEG_MAX_WIDTH);
        let jpeg_scaler_context = scaling::Context::get(
            decoder.format(),
            decoder.width(),
            decoder.height(),
            format::Pixel::RGB24,
            output_props.width,
            output_props.height,
            scaling::Flags::BILINEAR,
        )
        .expect("Failed to initialize JPEG scaler context");
        let jpeg_scaler_context = Rc::new(RefCell::new(jpeg_scaler_context));
        let packets = self.ictx.packets();
        SourceFrameIterator {
            packets,
            decoder,
            video_stream_index: self.video_stream_index,
            jpeg_scaler_context,
        }
    }

    pub fn properties(&self) -> VideoProperties {
        let decoder = self.video_decoder();
        VideoProperties {
            height: decoder.height(),
            width: decoder.width(),
            format: decoder.format(),
            time_base: decoder.time_base(),
            color_space: decoder.color_space(),
            color_range: decoder.color_range(),
        }
    }
}

pub struct SourceFrameIterator<'a> {
    decoder: decoder::Video,
    packets: format::context::input::PacketIter<'a>,
    video_stream_index: usize,
    jpeg_scaler_context: Rc<RefCell<scaling::Context>>,
}

impl<'a> Iterator for SourceFrameIterator<'a> {
    type Item = SourceFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let mut receive_frames = |decoder: &mut decoder::Video| -> Option<Self::Item> {
            let time_base = decoder.time_base().unwrap(); // TODO
            let timebase_ns =
                (time_base.numerator() as i64 * 1_000_000_000) / time_base.denominator() as i64;
            let jpeg_scaler_context = self.jpeg_scaler_context.clone();

            let mut frame = frame::Video::empty();
            for res in self.packets.by_ref() {
                let (stream, packet) = res.unwrap(); // TODO: handle error properly
                if stream.index() == self.video_stream_index {
                    decoder.send_packet(&packet).unwrap(); // TODO: handle error properly
                    if decoder.receive_frame(&mut frame).is_ok() {
                        let ts_ns = frame.pts().unwrap() * timebase_ns;
                        return Some(Self::Item {
                            frame,
                            ts_ns,
                            jpeg_scaler_context,
                        });
                    }
                }
            }
            None
        };

        if let Some(source_frame) = receive_frames(&mut self.decoder) {
            return Some(source_frame);
        }

        self.decoder.send_eof().unwrap(); // TODO: handle error properly
        receive_frames(&mut self.decoder)
    }
}

impl SourceFrame {
    pub fn encode_jpeg(&self) -> Vec<u8> {
        let mut scaler = self.jpeg_scaler_context.try_borrow_mut().unwrap();

        let mut rgb_frame = frame::Video::empty();
        scaler
            .run(&self.frame, &mut rgb_frame)
            .expect("Failed to scale video frame for JPEG");

        let mut res = vec![];

        let encoder = jpeg_encoder::Encoder::new(&mut res, JPEG_QUALITY);

        encoder
            .encode(
                rgb_frame.data(0),
                rgb_frame.width() as u16,
                rgb_frame.height() as u16,
                jpeg_encoder::ColorType::Rgb,
            )
            .expect("Failed to encode JPEG frame");

        res
    }
}

impl PartialEq for SourceFrame {
    fn eq(&self, other: &Self) -> bool {
        self.ts_ns == other.ts_ns && self.frame == other.frame
    }
}

impl Eq for SourceFrame {}

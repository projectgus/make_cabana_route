// Copyright (c) 2023 Angus Gratton
// SPDX-License-Identifier: GPL-2.0-or-later
use anyhow::{Context, Result};
use ffmpeg::frame::Video;
use ffmpeg::{
    codec, decoder, encoder, format, frame, media, software::scaling, Dictionary, Packet, Rational,
};
use jpeg_encoder;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

const TARGET_FPS: u32 = 20;

const TARGET_FRAME_NS: i64 = 1_000_000_000i64 / TARGET_FPS as i64;

const JPEG_QUALITY: u8 = 80;

// TODO: consider making these runtime configurable
const JPEG_MAX_WIDTH: u32 = 640;
/// Maximum width of an embedded JPEG thumbnail
const VIDEO_MAX_WIDTH: u32 = 1280;
/// Maximum width of the output video frame

pub struct SegmentVideoEncoder {
    octx: format::context::Output,
    scaler: Option<scaling::Context>,
    encoder: encoder::Video,
    video_stream_index: usize,
    frame_count: usize,
    pkt_count: usize,
}

impl SegmentVideoEncoder {
    pub fn new(path: &Path, properties: &VideoProperties, dump_info: bool) -> Result<Self> {
        let mut octx = format::output(path)
            .with_context(|| format!("Failed to create output context for {:?}", path))?;

        let mut ost = octx.add_stream()?;
        let video_stream_index = ost.index();

        let codec = encoder::find(codec::Id::HEVC).context("Failed to find HEV codec")?;
        let mut video = codec::Encoder::new(codec)
            .context("Failed to instantiate HEVC Codec")?
            .video()
            .context("Failed to get video from Codec")?;

        let scaler = if properties.width > VIDEO_MAX_WIDTH {
            // Scale the output video
            let scaled = properties.scale_to_width(VIDEO_MAX_WIDTH);
            eprintln!(
                "Scaling from {}x{} to {}x{}",
                properties.width, properties.height, scaled.width, scaled.height
            );

            video.set_height(scaled.height);
            video.set_width(scaled.width);
            video.set_format(scaled.format);

            Some(scaling::Context::get(
                properties.format,
                properties.width,
                properties.height,
                scaled.format,
                scaled.width,
                scaled.height,
                scaling::Flags::BILINEAR,
            )?)
        } else {
            // Don't need to scale
            video.set_height(properties.height);
            video.set_width(properties.width);
            video.set_format(properties.format);
            None
        };

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
        octx.write_header().context("Failed to write HEVC header")?;

        Ok(Self {
            octx,
            scaler,
            encoder,
            video_stream_index,
            frame_count: 0,
            pkt_count: 0,
        })
    }

    pub fn send_frame(&mut self, frame: &SourceFrame) -> Result<()> {
        let mut scaled_frame = Video::empty();
        let iframe = {
            if let Some(scaler) = &mut self.scaler {
                scaler.run(&frame.frame, &mut scaled_frame)?;
                &scaled_frame
            } else {
                &frame.frame
            }
        };

        self.encoder
            .send_frame(&iframe)
            .context("Failed to send frame to encoder")?;
        self.receive_packets()
            .context("Failed to read input video packets")?;
        self.frame_count += 1;
        Ok(())
    }

    fn receive_packets(&mut self) -> Result<()> {
        let mut encoded = Packet::empty();
        while self.encoder.receive_packet(&mut encoded).is_ok() {
            self.pkt_count += 1;
            encoded.set_stream(self.video_stream_index);
            encoded
                .write_interleaved(&mut self.octx)
                .context("failed to write to encoder")?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.encoder.send_eof().context("Failed to send EOF")?;
        self.receive_packets()
            .context("Failed to receive final packets")?;
        self.octx
            .write_trailer()
            .context("Failed to write trailer")?;
        Ok(())
    }
}

pub struct SourceVideo {
    video_file: PathBuf,
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
    pub fn new(video_file: &Path) -> Result<Self> {
        let ictx = format::input(video_file)
            .with_context(|| format!("Failed to open video file {:?}", video_file))?;
        let input = ictx
            .streams()
            .best(media::Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)
            .with_context(|| format!("Video file {:?} contained no video streams", video_file))?;
        let video_stream_index = input.index();

        Ok(Self {
            ictx,
            video_stream_index,
            video_file: video_file.to_path_buf(),
        })
    }

    pub fn video_decoder(&self) -> Result<decoder::Video> {
        self.ictx
            .streams()
            .best(media::Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)
            .with_context(|| format!("Input video {:?} has no video stream", self.video_file))?
            .decoder()
            .with_context(|| format!("Failed to create decoder for {:?}", self.video_file))?
            .open()
            .with_context(|| format!("Failed to open video decoder for {:?}", self.video_file))?
            .video()
            .with_context(|| format!("Failed to access video for {:?}", self.video_file))
    }

    // Didn't have any luck implementing IntoIter for this, but this is kind of better
    // as more flexible
    pub fn video_frames(&mut self) -> Result<SourceFrameIterator<'_>> {
        let decoder = self.video_decoder()?;
        let output_props = self.properties()?.scale_to_width(JPEG_MAX_WIDTH);
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
        Ok(SourceFrameIterator {
            packets,
            decoder,
            video_stream_index: self.video_stream_index,
            jpeg_scaler_context,
            next_frame_ts: 0,
        })
    }

    pub fn properties(&self) -> Result<VideoProperties> {
        let decoder = self.video_decoder()?;
        Ok(VideoProperties {
            height: decoder.height(),
            width: decoder.width(),
            format: decoder.format(),
            time_base: decoder.time_base(),
            color_space: decoder.color_space(),
            color_range: decoder.color_range(),
        })
    }
}

pub struct SourceFrameIterator<'a> {
    decoder: decoder::Video,
    packets: format::context::input::PacketIter<'a>,
    video_stream_index: usize,
    jpeg_scaler_context: Rc<RefCell<scaling::Context>>,
    next_frame_ts: i64,
}

impl<'a> Iterator for SourceFrameIterator<'a> {
    type Item = SourceFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let mut receive_frames = |decoder: &mut decoder::Video| -> Option<Self::Item> {
            let time_base = decoder.time_base().expect("Video must have time base");
            let timebase_ns =
                (time_base.numerator() as i64 * 1_000_000_000) / time_base.denominator() as i64;
            let jpeg_scaler_context = self.jpeg_scaler_context.clone();

            let mut frame = frame::Video::empty();
            for res in self.packets.by_ref() {
                let (stream, packet) = res.expect("Failed to iterate frames");
                if stream.index() == self.video_stream_index {
                    decoder
                        .send_packet(&packet)
                        .expect("Failed to decode frames");
                    if decoder.receive_frame(&mut frame).is_ok() {
                        let ts_ns = frame.pts().unwrap() * timebase_ns;
                        // Drop frames as needed to meet the target FPS rate
                        if ts_ns >= self.next_frame_ts + TARGET_FRAME_NS {
                            self.next_frame_ts = if self.next_frame_ts == 0 {
                                ts_ns + TARGET_FRAME_NS
                            } else {
                                self.next_frame_ts + TARGET_FRAME_NS
                            };
                            return Some(Self::Item {
                                frame,
                                ts_ns,
                                jpeg_scaler_context,
                            });
                        }
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

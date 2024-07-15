// Copyright (c) 2023 Angus Gratton
// SPDX-License-Identifier: GPL-2.0-or-later
use anyhow::{Context, Result};
use ffmpeg::ffi::AVPixelFormat;
use ffmpeg::filter;
use ffmpeg::format::Pixel;
use ffmpeg::{
    codec, decoder, encoder, format, frame, media, software::scaling, Dictionary, Packet, Rational,
};
use jpeg_encoder;
use std::path::{Path, PathBuf};

const TARGET_FPS: u32 = 20;

const TARGET_FRAME_NS: i64 = 1_000_000_000i64 / TARGET_FPS as i64;

const JPEG_QUALITY: u8 = 80;

// TODO: consider making these runtime configurable
const JPEG_MAX_WIDTH: u32 = 640;
/// Maximum width of an embedded JPEG thumbnail
const VIDEO_MAX_WIDTH: u32 = 1280;
/// Maximum width of the output video frame

struct FilterGraph {
    graph: ffmpeg::filter::graph::Graph,
}

pub struct SegmentVideoEncoder {
    octx: format::context::Output,
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

        video.set_width(properties.out_width);
        video.set_height(properties.out_height);
        video.set_format(properties.format);
        video.set_frame_rate(Some(Rational::new(TARGET_FPS as i32, 1)));
        video.set_colorspace(properties.color_space);
        video.set_color_range(properties.color_range);

        // This time base seems to be required by HEVC, but unsure how it's supposed
        // to be set
        if let Some(time_base) = properties.time_base {
            video.set_time_base(Some(time_base.invert()));
        } else {
            video.set_time_base(Some(Rational::new(30000, 1)));
        }
        video.set_flags(codec::Flags::GLOBAL_HEADER);

        eprintln!("Writing segment video to {}...", path.display());

        let mut x265_opts = Dictionary::new();
        x265_opts.set("preset", "medium"); // default is medium. TODO: make configurable?
        x265_opts.set("crf", "28"); // default is 28. lower == higher quality, bigger files.
        let encoder = video
            .open_with(x265_opts)
            .expect("error opening HEVC encoder");
        ost.set_parameters(encoder.parameters());

        if dump_info {
            format::context::output::dump(&octx, 0, path.to_str());
        }
        octx.write_header().context("Failed to write HEVC header")?;

        Ok(Self {
            octx,
            encoder,
            video_stream_index,
            frame_count: 0,
            pkt_count: 0,
        })
    }

    pub fn send_frame(&mut self, frame: &SourceFrame) -> Result<()> {
        self.encoder
            .send_frame(&frame.frame)
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
    out_height: u32,
    out_width: u32,
    format: format::Pixel,
    time_base: Option<Rational>,
    color_space: ffmpeg::color::Space,
    color_range: ffmpeg::color::Range,
}

pub struct SourceFrame {
    pub frame: frame::Video,
    pub ts_ns: i64,
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
        let props = self.properties()?;

        let mut filter_spec = format!("scale={}:{}", props.out_width, props.out_height);

        let rotate = self.display_rotation()?;
        if rotate != 0 {
            filter_spec = format!("{},rotate={}*PI/180", filter_spec, rotate);
        }
        eprintln!("Filter spec: {}", filter_spec);

        let filter_graph = FilterGraph::new(&decoder, &filter_spec)?;
        let packets = self.ictx.packets();

        Ok(SourceFrameIterator {
            packets,
            decoder,
            video_stream_index: self.video_stream_index,
            next_frame_ts: 0,
            filter_graph,
        })
    }

    fn display_rotation(&self) -> Result<i32> {
        let stream = self
            .ictx
            .streams()
            .best(media::Type::Video)
            .ok_or(ffmpeg::Error::StreamNotFound)?;
        Ok(stream.display_rotation() as i32)
    }

    pub fn properties(&self) -> Result<VideoProperties> {
        let decoder = self.video_decoder()?;

        let in_width = decoder.width();
        let in_height = decoder.height();
        let out_width = if in_width > VIDEO_MAX_WIDTH {
            VIDEO_MAX_WIDTH
        } else {
            in_width
        };
        let out_height = out_width * in_height / in_width;

        Ok(VideoProperties {
            out_width,
            out_height,
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
    filter_graph: FilterGraph,
    next_frame_ts: i64,
}

impl<'a> Iterator for SourceFrameIterator<'a> {
    type Item = SourceFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let mut receive_frames = |decoder: &mut decoder::Video| -> Option<Self::Item> {
            let time_base = decoder.time_base().expect("Video must have time base");
            let timebase_ns =
                (time_base.numerator() as i64 * 1_000_000_000) / time_base.denominator() as i64;
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
                            frame.set_kind(ffmpeg::picture::Type::None);
                            self.filter_graph
                                .filter_frame(&mut frame)
                                .expect("Failed to filter frame");
                            return Some(Self::Item { frame, ts_ns });
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
        // JPEG scaler context takes output of the filter graph pipeline as
        // input. Uses a simple swscaler context rather than a more complex
        // av_filter pipeline.
        //
        // Making a new scaler context for each JPEG may seem wasteful, but none
        // of this code shows up at all in performance profiling...
        let jpeg_width = JPEG_MAX_WIDTH.min(self.frame.width());
        let jpeg_height = jpeg_width * self.frame.height() / self.frame.width();
        let mut scaler = scaling::Context::get(
            self.frame.format(),
            self.frame.width(),
            self.frame.height(),
            format::Pixel::RGB24,
            jpeg_width,
            jpeg_height,
            scaling::Flags::BILINEAR,
        )
        .expect("Failed to initialize JPEG scaler context");

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

impl FilterGraph {
    const IN: &'static str = "in";
    const OUT: &'static str = "out";

    fn new(decoder: &decoder::Video, filter_spec: &str) -> Result<Self> {
        let buffer_src = filter::find("buffer").context("can't find src")?;
        let buffer_sink = filter::find("buffersink").context("can't find sink")?;
        let mut graph = filter::graph::Graph::new();

        let time_base = decoder.time_base().unwrap();
        let mut pixel_aspect = decoder.aspect_ratio();
        if pixel_aspect.numerator() == 0 {
            // For some reason, ffmpeg sometimes returns 0/0 here
            pixel_aspect = Rational::new(1, 1);
        }

        let src_args = format!(
            "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect={}/{}",
            decoder.width(),
            decoder.height(),
            <Pixel as Into<AVPixelFormat>>::into(decoder.format()) as i32,
            time_base.numerator(),
            time_base.denominator(),
            pixel_aspect.numerator(),
            pixel_aspect.denominator(),
        );

        // Note: inefficient that we add here and then have to call get() for each
        // frame. However, otherwise FilterGraph has to become a self-referential struct.
        let mut buffer_src_ctx = graph
            .add(&buffer_src, Self::IN, &src_args)
            .with_context(|| format!("Failed to add src {}", src_args))?;
        buffer_src_ctx.set_pixel_format(decoder.format());

        let mut buffer_sink_ctx = graph
            .add(&buffer_sink, Self::OUT, "")
            .context("Failed to add sink")?;
        buffer_sink_ctx.set_pixel_format(decoder.format());

        // Link up start and end (unclear why output is the IN here).
        graph
            .output(Self::IN, 0)
            .context("Failed to allocate output")?
            .input(Self::OUT, 0)
            .context("Failed to allocate input")?
            .parse(filter_spec)
            .context("Failed to parse filter spec")?;
        graph.validate().context("Filter graph not valid")?;

        Ok(FilterGraph { graph })
    }

    fn filter_frame(&mut self, frame: &mut frame::Video) -> Result<()> {
        let mut src_ctx = self.graph.get(Self::IN).unwrap();
        let mut src = src_ctx.source();
        src.add(&frame.0).context("Failed to add to source")?;

        let mut sink_ctx = self.graph.get(Self::OUT).unwrap();
        let mut sink = sink_ctx.sink();
        sink.frame(&mut frame.0)
            .context("Failed to read from sink")?;
        Ok(())
    }
}

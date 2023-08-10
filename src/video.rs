use std::path::Path;
use std::rc::Rc;
use std::{cell::RefCell, error::Error};

use ffmpeg::{
    codec, decoder, encoder, format, frame, media, software::scaling, Dictionary, Packet, Rational,
};
use jpeg_encoder;

const TARGET_FPS: u32 = 20;

const JPEG_QUALITY: u8 = 80;

pub struct SegmentVideoEncoder {
    octx: format::context::Output,
    encoder: encoder::Video,
    video_stream_index: usize,
    frame_count: usize,
    pkt_count: usize,
}

impl SegmentVideoEncoder {
    pub fn new(path: &Path, properties: &VideoProperties) -> Result<Self, Box<dyn Error>> {
        let mut octx = format::output(&path).unwrap();

        let mut ost = octx.add_stream()?;
        let video_stream_index = ost.index();

        let codec = encoder::find(codec::Id::HEVC).unwrap();
        let mut video = codec::Encoder::new(codec)?.video()?;

        video.set_height(properties.height);
        video.set_width(properties.width);
        video.set_aspect_ratio(properties.aspect_ratio);
        video.set_format(properties.format);
        video.set_frame_rate(Some(Rational::new(TARGET_FPS as i32, 1)));
        video.set_colorspace(properties.color_space);
        video.set_color_range(properties.color_range);

        // This time base seems to be required by HEVC, but unsure how it's supposed
        // to be set
        video.set_time_base(properties.time_base.invert()); //Rational::new(1, 90000));
        video.set_flags(codec::Flags::GLOBAL_HEADER);

        eprintln!("Writing segment video to {}...", path.display());

        let mut x264_opts = Dictionary::new();
        x264_opts.set("preset", "medium");
        let encoder = video.open().expect("error opening HEVC encoder");
        ost.set_parameters(encoder.parameters());

        //octx.set_metadata(metadata);
        format::context::output::dump(&mut octx, 0, path.to_str());
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
        //dbg!(self.frame_count);
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
            //dbg!(encoded.pts(), encoded.dts());
            //encoded.rescale_ts(self.decoder_time_base, self.encoder.time_base());
            //dbg!(encoded.pts(), encoded.dts());
            encoded.write_interleaved(&mut self.octx).unwrap();
        }

        Ok(())
    }

    pub fn finish(mut self) {
        self.encoder.send_eof().unwrap();
        self.receive_packets().unwrap();
        self.octx.write_trailer().unwrap();
        dbg!(self.frame_count, self.pkt_count);
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
#[derive(Clone)]
pub struct VideoProperties {
    height: u32,
    width: u32,
    aspect_ratio: Rational,
    format: format::Pixel,
    time_base: Rational,
    color_space: ffmpeg::color::Space,
    color_range: ffmpeg::color::Range,
}

pub struct SourceFrame {
    pub frame: frame::Video,
    pub ts_ns: i64,
    jpeg_scaler_context: Rc<RefCell<scaling::Context>>,
}

impl SourceVideo {
    pub fn new(video_file: &Path) -> Result<Self, Box<dyn Error>> {
        let ictx = format::input(&video_file)?;
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
        let decoder = input.decoder().unwrap().open().unwrap().video().unwrap(); // TODO: error handling!
        decoder
    }

    // Didn't have any luck implementing IntoIter for this, but this is kind of better
    // as more flexible
    pub fn video_frames(&mut self) -> SourceFrameIterator<'_> {
        let decoder = self.video_decoder();
        let jpeg_scaler_context = scaling::Context::get(
            decoder.format(),
            decoder.width(),
            decoder.height(),
            format::Pixel::RGB24,
            decoder.width(), // TODO: decide if we actually should scale down
            decoder.height(),
            scaling::Flags::BILINEAR,
        )
        .unwrap(); // TODO: better error handler here?
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
            aspect_ratio: decoder.aspect_ratio(),
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
            let time_base = decoder.time_base();
            let timebase_ns =
                (time_base.numerator() as i64 * 1000_000_000) / time_base.denominator() as i64;
            let jpeg_scaler_context = self.jpeg_scaler_context.clone();

            let mut frame = frame::Video::empty();
            while let Some(res) = self.packets.next() {
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
        return receive_frames(&mut self.decoder);
    }
}

impl SourceFrame {
    pub fn encode_jpeg(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut scaler = self.jpeg_scaler_context.try_borrow_mut()?;

        let mut rgb_frame = frame::Video::empty();
        scaler.run(&self.frame, &mut rgb_frame)?;

        let mut res = vec![];

        let encoder = jpeg_encoder::Encoder::new(&mut res, JPEG_QUALITY);

        encoder.encode(
            rgb_frame.data(0),
            self.frame.width() as u16,
            self.frame.height() as u16,
            jpeg_encoder::ColorType::Rgb,
        )?;

        Ok(res)
    }
}

impl PartialEq for SourceFrame {
    fn eq(&self, other: &Self) -> bool {
        return self.ts_ns == other.ts_ns && self.frame == other.frame;
    }
}

impl Eq for SourceFrame {}

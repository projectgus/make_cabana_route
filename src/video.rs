use std::error::Error;
use std::path::Path;

use ffmpeg::{codec, decoder, encoder, format, frame, media, Dictionary, Packet, Rational};

const TARGET_FPS: u32 = 20;
const SEGMENT_SECS: i64 = 60;

pub struct SegmentVideoEncoder {
    octx: format::context::Output,
    encoder: encoder::Video,
    decoder_time_base: Rational,
    video_stream_index: usize,
    frame_count: usize,
    pkt_count: usize,
}

impl SegmentVideoEncoder {
    pub(crate) fn new(path: &Path, source: &SourceVideo) -> Result<Self, Box<dyn Error>> {
        let mut octx = format::output(&path).unwrap();

        let mut ost = octx.add_stream()?;
        let video_stream_index = ost.index();

        let codec = encoder::find(codec::Id::HEVC).unwrap();
        let mut encoder = codec::Encoder::new(codec)?.video()?;

        let decoder = source.video_decoder();

        encoder.set_height(decoder.height());
        encoder.set_width(decoder.width());
        encoder.set_aspect_ratio(decoder.aspect_ratio());
        encoder.set_format(decoder.format());
        encoder.set_frame_rate(Some(Rational::new(TARGET_FPS as i32, 1)));
        encoder.set_colorspace(decoder.color_space());
        encoder.set_color_range(decoder.color_range());

        // This time base seems to be required by HEVC, but unsure how it's supposed
        // to be set
        encoder.set_time_base(decoder.time_base().invert()); //Rational::new(1, 90000));
        encoder.set_flags(codec::Flags::GLOBAL_HEADER);

        eprintln!("Writing segment video to {}...", path.display());

        let mut x264_opts = Dictionary::new();
        x264_opts.set("preset", "medium");
        let encoder = encoder.open().expect("error opening HEVC encoder");
        ost.set_parameters(encoder.parameters());

        //octx.set_metadata(metadata);
        format::context::output::dump(&mut octx, 0, path.to_str());
        octx.write_header().unwrap();

        Ok(Self {
            octx,
            encoder,
            decoder_time_base: decoder.time_base(),
            video_stream_index,
            frame_count: 0,
            pkt_count: 0,
        })
    }

    pub(crate) fn send_frame(&mut self, frame: &SourceFrame) -> Result<(), Box<dyn Error>> {
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
            encoded.rescale_ts(self.decoder_time_base, self.encoder.time_base());
            //dbg!(encoded.pts(), encoded.dts());
            encoded.write_interleaved(&mut self.octx).unwrap();
        }

        Ok(())
    }

    pub(crate) fn finish(mut self) {
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

#[derive(Eq, PartialEq)]
pub struct SourceFrame {
    pub frame: frame::Video,
    pub ts_ns: i64,
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
        let packets = self.ictx.packets();
        SourceFrameIterator {
            packets,
            decoder,
            video_stream_index: self.video_stream_index,
        }
    }
}

pub struct SourceFrameIterator<'a> {
    decoder: decoder::Video,
    packets: format::context::input::PacketIter<'a>,
    video_stream_index: usize,
}

impl<'a> Iterator for SourceFrameIterator<'a> {
    type Item = SourceFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let mut receive_frames = |decoder: &mut decoder::Video| -> Option<Self::Item> {
            let time_base = decoder.time_base();
            let timebase_ns =
                (time_base.numerator() as i64 * 1000_000_000) / time_base.denominator() as i64;

            let mut frame = frame::Video::empty();
            while let Some(res) = self.packets.next() {
                let (stream, packet) = res.unwrap(); // TODO: handle error properly
                if stream.index() == self.video_stream_index {
                    decoder.send_packet(&packet).unwrap(); // TODO: handle error properly
                    if decoder.receive_frame(&mut frame).is_ok() {
                        let ts_ns = frame.pts().unwrap() * timebase_ns;
                        return Some(Self::Item { frame, ts_ns });
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

// impl RouteThumbs {
//     /* This function is embarassingly messy, as it tries to do two things at once:

//     - Transcode the video_file to 20fps segment videos of length SEGMENT_SECS
//     - Build up the list of thumbnail images from every second of footage

//     ... and there is no doubt an elegant way to do this with an iterator of decoded frames,
//     but I'm not good enough at either Rust or libavcodec to make that work for now. So we get the
//     messy imperative-ish C-by-any-other-language version...
//      */
//     pub fn new(video_file: &Path, route_dir_base: &str) -> Result<Self, Box<dyn Error>> {
//         let mut ictx = format::input(&video_file)?;
//         let input = ictx
//             .streams()
//             .best(media::Type::Video)
//             .ok_or(ffmpeg::Error::StreamNotFound)?;
//         let video_stream_index = input.index();

//         let mut decoder = input.decoder()?.open()?.video()?;
//         let timebase = input.time_base();
//         dbg!(timebase);

//         // Calculate the PTS interval at which to keep frames for TARGET_FPS
//         let pts_per_out_frame =
//             timebase.denominator() as i64 / timebase.numerator() as i64 / TARGET_FPS as i64;

//         // Decoder state tracking
//         let mut frame_index = 0;
//         let mut next_pts = 0;

//         // Return the path to the output video for segment 'num'
//         let get_segment_video_path = |num: i64| -> PathBuf {
//             let base = format!("{}-{:02}", route_dir_base, num);
//             let mut result = PathBuf::from(base);
//             std::fs::create_dir_all(&result).ok(); // TODO
//             result.push("qcamera.ts");
//             result
//         };

//         // Encoder state tracking, for each segment in the route
//         let mut segment = 0;
//         let mut segment_encode =
//             SegmentVideoEncoder::new(&get_segment_video_path(segment), &decoder).unwrap();
//         let time_per_segment = (SEGMENT_SECS as f64 / f64::from(timebase)).round() as i64;

//         let mut receive_and_process_decoded_frames =
//             |decoder: &mut ffmpeg::decoder::Video| -> Result<(), ffmpeg::Error> {
//                 let mut frame = frame::Video::empty();
//                 while decoder.receive_frame(&mut frame).is_ok() {
//                     if let Some(pts) = frame.pts() {
//                         if pts >= next_pts {
//                             let timestamp = frame.timestamp();
//                             if timestamp.unwrap_or(0) > time_per_segment * (segment + 1) {
//                                 eprintln!("Finishing segment at frame_index {}", frame_index);
//                                 // This segment is finished, start another one
//                                 segment += 1;
//                                 segment_encode.finish();

//                                 segment_encode = SegmentVideoEncoder::new(
//                                     &get_segment_video_path(segment),
//                                     &decoder,
//                                 )
//                                 .unwrap();
//                             }

//                             frame.set_kind(picture::Type::None);
//                             segment_encode.send_frame(&frame).ok(); // TODO!

//                             next_pts += pts_per_out_frame;
//                         }
//                         frame_index += 1;
//                     }
//                 }
//                 Ok(())
//             };

//         for res in ictx.packets() {
//             let (stream, packet) = res.unwrap();
//             if stream.index() == video_stream_index {
//                 decoder.send_packet(&packet)?;
//                 receive_and_process_decoded_frames(&mut decoder)?;
//             }
//         }
//         decoder.send_eof()?;
//         receive_and_process_decoded_frames(&mut decoder)?;

//         segment_encode.finish();

//         Ok(RouteThumbs {})
//     }
// }

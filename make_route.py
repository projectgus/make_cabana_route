#!/usr/bin/env python
"""Generate a route for local hacked cabana to read."""
import csv
import bz2
import os.path
import datetime
import re
import subprocess
import tempfile
import sys
import capnp
import yaml

capnp.remove_import_hook()
log_capnp = capnp.load("../openpilot/cereal/log.capnp")  # TODO: get this path from somewhere
log_car = capnp.load("../openpilot/cereal/car.capnp")

SentinelType = log_capnp.Sentinel.SentinelType

SEGMENT_LEN_S = 60
SEGMENT_LEN_NS = int(60*1e9)

VIDEO_FPS = 20


def make_route(car_info, log_file, video_file, sync, data_dir):
    """Convert video & log CSV files to a "route" in a directory for Cabana."""

    # For now, use the creation file of the log file as the timestamp.
    # However, might be nice to make an option to specify this in the YML
    route_ts = datetime.datetime.fromtimestamp(os.path.getctime(log_file))

    # See replay Route::parseRoute() for the regex that loads the route name.
    #
    # There is an optional 16 character alphanumeric prefix field with the dongle ID.
    # Currently leave this off, it looks like Cabana may be happy without it.
    #
    # Similarly, we leave off the optional --<segment_num> suffix
    route_name = route_ts.strftime("%Y-%m-%d--%H-%M-%S")

    print(f"Generating route {route_name}...")

    # Format for route directory holding each segment in the route. route name plus segment number
    segment_dir_format = os.path.join(data_dir, route_name) + "--{}"

    video_length = get_video_length(video_file)

    # timestamp (ns) in video where you see the CAN timestamp, relative to start of video
    video_start_ts = int(sync[0] * 1e9)
    # timestamp (ns) of CAN message @ video_start_ts in the video
    log_start_ts = sync[1] * 1000

    # CAN timestamp at 0:00.000 in the video. Will be LogMonoTime for start of route
    route_init_ts = log_start_ts - video_start_ts

    # (note: unfortunately we have to drop any CAN messages that appear before
    # route_init_ts in the CAN log at the moment.  we might be able to generate
    # some blank frames to make up for this, but not tried yet.)

    segment_dirs = write_logs(log_file, segment_dir_format, car_info, route_init_ts, video_length)
    write_videos(video_file, segment_dirs)


def get_video_length(video_file):
    """Return length of a video file in seconds (float)"""
    res = subprocess.run(["ffprobe", video_file],
                         capture_output=True,
                         check=True,
                         encoding="utf8")
    duration = re.search(r"Duration: *(\d+):(\d+):(\d+)\.(\d+)", res.stderr)
    if not duration:
        raise SystemExit(f"Failed to find duration of video {video_file}")
    hours = int(duration.group(1))
    minutes = int(duration.group(2))
    seconds = int(duration.group(3))
    millis = int(duration.group(4))
    length = millis/1000 + seconds + (minutes * 60) + (hours * 60 * 60)
    print(f"Video {video_file} length {length}s")
    return length


def get_first_can_ts(csv_file):
    """Open the CSV and get the first CAN log timestamp in nanoseconds, minus one."""
    with open(csv_file, encoding="ascii") as csvf:
        reader = csv.reader(csvf)
        next(reader)  # skip header
        first_line = next(reader)
        return int(first_line[0]) * 1000 - 1


def read_csv_messages(csv_file):
    """Generate CAN message data from the CSV file entries.

    The annoying property here is the need to sort the events by timestamp, as
    in busy periods bus 0 & 1 will sometimes produce out of order log entries.
    """

    def unsorted():
        with open(csv_file, encoding="ascii") as csvf:
            reader = csv.reader(csvf)
            next(reader)  # skip the header

            for line in reader:
                yield (
                    int(line[0]) * 1000,  # ts (ns)
                    int(line[3]),  # bus
                    int(line[1], 16),  # address
                    bytes([int(b, 16) for b in line[5:]]),
                )  # data

    return sorted(unsorted(), key=lambda m: m[0])


def write_logs(csv_file, segment_path_format, car_info, route_init_ts, video_length):
    """Write rlog files for each segment, based on the CAN messages from csv_file."""

    segments = []

    with tempfile.TemporaryFile("a+b") as rlog:
        # Note: pycapnp doesn't support writing to a buffer object like a bz2
        # file, just to a plain file (uses fileno). So we use a plain
        # temporary file 'rlog', then read its contents back
        # out and compress it to a bz2 file.

        def write_event(logMonoTime, **kwargs):
            """Write a new event to rlog."""
            log_capnp.Event.new_message(
                logMonoTime=logMonoTime, valid=True, **kwargs
            ).write(rlog)

        def write_sentinel(logMonoTime, sentinelType):
            """Write a Sentinel marker event in the log.

            Not clear if Cabana actually cares about these.
            """
            write_event(logMonoTime,
                        sentinel=log_capnp.Sentinel.new_message(
                            type=sentinelType))

        # Flush log data to a new compressed file
        def flush_rlog():
            # make the parent directory for the new route segment
            segment_dir = segment_path_format.format(len(segments))
            os.makedirs(segment_dir, exist_ok=True)
            segments.append(segment_dir)

            rlog_path = os.path.join(segment_dir, "rlog.bz2")
            with bz2.open(rlog_path, "wb", compresslevel=6) as rlog_bz:
                rlog.seek(0)
                rlog_bz.write(rlog.read())
                rlog.truncate(0)
                rlog.seek(0)

        # Write a dummy InitData that includes route_init_ts as its logMonoTime
        # cabana's rlog-downloader will use this as the start time for the route
        # (routeInitTime), which doubles as the start time for the video.
        #
        # (There is support in rlog-downloader for reading a firstFrameTime from
        # a Frame object that would be used to calculate a videoOffset, but it
        # seems this is no longer supported in the current log.capnp file so
        # videoOffset==0 in cabana.)
        #
        # Cabana doesn't seem to read any of the other InitData struct fields.
        write_event(route_init_ts, initData=log_capnp.InitData.new_message())

        # Write out video frame indexes for duration of the video
        video_end_ts = route_init_ts + int(1e9 * video_length)
        next_frame_ts = route_init_ts
        next_frame_id = 0

        if car_info:
            # barebones carParams
            name, details = car_info
            write_event(
                route_init_ts + 1,
                carParams=log_car.CarParams.new_message(
                    carName=f"{name} {details}",
                    # cabana uses this to find a matching DBC file in localstorage on load (maybe, hopefully)
                    carFingerprint=name,
                ),
            )

        write_sentinel(route_init_ts + 2, SentinelType.startOfRoute)

        # Read CAN messages the CSV CAN log and build Can Events for rlog
        segment_ts = route_init_ts
        dropped = 0
        event_ts = None
        can_data = []

        for ts, bus, address, data in read_csv_messages(csv_file):
            # drop any messages from before the video started
            if ts < route_init_ts:
                dropped += 1
                continue

            # event_ts is the first message in the next Event we write out
            if event_ts is None:
                event_ts = ts

                # Write a video encode idx if new frame is coming up
                if event_ts >= next_frame_ts and event_ts < video_end_ts:
                    timestampEof = next_frame_ts + int(1e9 / VIDEO_FPS) - 1
                    FRAMES_PER_SEGMENT = SEGMENT_LEN_S * VIDEO_FPS
                    segmentId = next_frame_id - (FRAMES_PER_SEGMENT * len(segments))
                    # Hack, don't write the last frame in each segment
                    if 0 <= segmentId < FRAMES_PER_SEGMENT:
                        write_event(next_frame_ts,
                                    roadEncodeIdx=log_capnp.EncodeIndex.new_message(
                                        frameId=next_frame_id,
                                        type=log_capnp.EncodeIndex.Type.fullHEVC,
                                        encodeId=next_frame_id,  # TBD if should be diff
                                        segmentNum=len(segments),
                                        segmentId=segmentId,
                                        segmentIdEncode=segmentId,  # TBD if should be diff
                                        timestampSof=next_frame_ts,
                                        timestampEof=timestampEof,
                                    ))
                        next_frame_ts = timestampEof + 1
                        next_frame_id += 1

                # Each segment should be at most 60s. If the next event will take us over
                # the 60s mark, write out this segment and start a new one
                if event_ts - segment_ts > SEGMENT_LEN_NS:
                    write_sentinel(event_ts - 2, SentinelType.endOfSegment)
                    flush_rlog()
                    # start next segment with blank initData and a sentinel
                    write_event(route_init_ts, initData=log_capnp.InitData.new_message())
                    write_sentinel(event_ts - 1, SentinelType.startOfSegment)
                    segment_ts = event_ts

            # BusTime seems to be in units of 2ms, truncated to 16-bit
            # (but also ignored by Cabana, I think)
            bus_time = (ts // 500_000) & 0xFFFF

            can_data.append(
                log_capnp.CanData.new_message(
                    address=address, busTime=bus_time, dat=data, src=bus
                ),  # I think that's what this one means
            )

            # Aim for each Event to contain around 10ms of messages
            if ts - event_ts > 10_000_000:
                # Flush the can_data to an Event
                if len(can_data) > 100:
                    # Even a very busy system shouldn't have too many messages in one event, may indicate a problem
                    print(f"WARNING: Flushing {len(can_data)} messages @ {event_ts}")
                write_event(event_ts, can=can_data)

                can_data = []
                event_ts = None

        # Create an event from any CAN messages left at the end
        if can_data:
            write_event(event_ts, can=can_data)

        print(f"Dropped {dropped} CAN messages from before 0:00.000 in video")

        # endOfRoute sentinel (Cabana seems to ignore this, also)
        write_sentinel(ts + 1, SentinelType.endOfRoute)

        # Flush to the final segment rlog.bz2 file
        flush_rlog()

        print(segments)
        print(f"Wrote {len(segments)} rlog files")
        return segments


def write_videos(video_file, segment_dirs):
    """Generate the qcamera.ts files for each segment."""

    for idx, segment_dir in enumerate(segment_dirs):
        ts_path = os.path.join(segment_dir, 'qcamera.ts')
        if os.path.exists(ts_path):
            print(
                f"Skipping transcoding {video_file} idx {idx} -> {ts_path}. "
                "Destination file exists. Delete to force transcode."
            )
            continue

        print(f"Transcoding {video_file} segment {idx}...")
        # Note: these options assume VAAPI accelerated h264 encoding is available & configured
        cmd = [
            "ffmpeg",
            "-hwaccel",
            "vaapi",
            "-hwaccel_output_format",
            "vaapi",
            "-ss",
            str(idx * SEGMENT_LEN_S),
            "-i",
            video_file,
            "-c:v",
            "hevc_vaapi",
            "-b:v",
            "500k",
            "-an",
            "-f", "mpegts",
            "-t", str(SEGMENT_LEN_S),
            "-r",
            str(VIDEO_FPS),
            ts_path,
        ]

        res = subprocess.run(cmd, capture_output=True, encoding='utf8', check=False)
        if res.returncode == 0:
            print("Transcoding finished.")
        elif idx > 0 and 'frame=    0' in res.stderr:  # TODO: make this less hacky
            print(f"Past end of input video at segment {idx}")
            return
        else:
            print(f"Transcoding failed. ffmpeg return code {res.returncode}")
            print(f'Command line: {" ".join(cmd)}')
            print(res.stdout)
            print(res.stderr)
            if os.path.exists(ts_path):
                os.unlink(ts_path)
            raise SystemExit(1)


def main():  # noqa: D103
    """ Generate routes """
    yaml_path = sys.argv[1]
    yaml_dir = os.path.dirname(yaml_path)

    data_dir = sys.argv[2]

    with open(yaml_path, encoding="utf8") as yamlfile:
        logs = yaml.load(yamlfile, yaml.Loader)

    for log in logs:
        log_file = os.path.normpath(os.path.join(yaml_dir, log["logfile"]))
        video = os.path.normpath(os.path.join(yaml_dir, log["video"]))
        print(log["sync"])
        sync = (log["sync"]["video_s"], log["sync"]["log_us"])
        make_route((log["car"], log["car_details"]), log_file, video, sync, data_dir)


if __name__ == "__main__":
    main()

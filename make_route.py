#!/usr/bin/env python
"""Generate a route for local hacked cabana to read."""
import csv
import gzip
import glob
import json
import os
import os.path
import datetime
import subprocess
import tempfile
import sys
import capnp
import yaml

capnp.remove_import_hook()
log_capnp = capnp.load("cereal/log.capnp")
log_car = capnp.load("cereal/car.capnp")

SentinelType = log_capnp.Sentinel.SentinelType


def make_route(car_info, logfile, videofile, sync, routes_dir):
    """Convert video & log CSV files to a "route" in a directory for Cabana."""
    # A comma.ai log would have an InitData event here, but trying to skip that one

    # the startTime variable in cabana is parsed from log name as YYYY-MM-DD--H-m-s
    # but year seems to be optional?
    ctime = datetime.datetime.fromtimestamp(os.path.getctime(logfile))
    route_name = "{}-{}".format(
        os.path.splitext(os.path.basename(logfile))[0],
        ctime.strftime("%Y-%m-%d--%H-%M-%S"),
    )

    route_dir = os.path.join(routes_dir, route_name)

    print(f"Generating route {route_dir}...")

    # timestamp (ns) in video where you see the CAN timestamp, relative to start of video
    video_start_ts = int(sync[0] * 1e9)
    # timestamp (ns) of CAN message @ video_start_ts in the video
    log_start_ts = sync[1] * 1000

    # CAN timestamp at 0:00.000 in the video
    route_init_ts = log_start_ts - video_start_ts

    # (note: unfortunately we have to drop any CAN messages that appear before
    # route_init_ts in the CAN log at the moment.  we might be able to generate
    # some blank frames to make up for this, but not tried yet.)

    os.makedirs(route_dir, exist_ok=True)

    nlogs = write_logs(logfile, os.path.join(route_dir, "rlog"), car_info, route_init_ts)
    write_route_json(os.path.join(route_dir, "route.json"), nlogs)
    write_stream(videofile, route_dir)


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


def write_logs(csv_file, rlog_base, car_info, route_init_ts):
    """Write one or more rlog files based on the CAN messages from csv_file."""
    nlogs = 0

    with tempfile.TemporaryFile("a+b") as rlog:
        # Note: pycapnp doesn't support writing to a buffer object like a gzip
        # or bz2 file, just to a plain file (uses fileno). So we use a plain
        # temporary file 'rlog', then periodically read its contents back
        # out and compress it to a gzip file.

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

        # Flush log data so far to a new compressed file
        def flush_rlog(nlogs):
            with gzip.open('{}{}.gz'.format(rlog_base, nlogs), "wb", compresslevel=6) as rlog_gz:
                rlog.seek(0)
                rlog_gz.write(rlog.read())
                rlog.truncate(0)
                rlog.seek(0)
            return nlogs + 1

        write_event(route_init_ts, initData=log_capnp.InitData.new_message())

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

                # Aim for each rlogNN.gz file to contain up to 4MB of unencrypted log data
                #
                # (real comma.ai logs seem much bigger, maybe 30MB-40MB
                # uncompressed. However they also contain a lot of other event types
                # cabana ignores. Our logs are basically 100% CAN messages.)
                if rlog.tell() > 4_000_000:
                    write_sentinel(event_ts + 1, SentinelType.endOfSegment)
                    nlogs = flush_rlog(nlogs)
                    # initData & startOfSegment sentinel goes into start of next segment
                    write_event(route_init_ts, initData=log_capnp.InitData.new_message())
                    write_sentinel(event_ts + 2, SentinelType.startOfSegment)

                event_ts = None
                can_data = []

        # Create an event from any CAN messages left at the end
        if can_data:
            write_event(event_ts, can=can_data)

        print(f"Dropped {dropped} CAN messages from before 0:00.000 in video")

        # endOfRoute sentinel (Cabana seems to ignore this, also)
        write_sentinel(ts + 1, SentinelType.endOfRoute)

        # Flush the events left at the end (at minimum, this is the sentinel event)
        nlogs = flush_rlog(nlogs)

        print(f'Wrote {nlogs} log segments for route')

        return nlogs


def write_route_json(json_path, nlogs):
    """Write the "fake API" route.json file with some metadata about the route."""
    doc = {
        # used in local_hacks/api.js getRouteFiles() to generate the correct
        # number of rlog files to stream in. Otherwise unused?
        "segment_numbers": list(range(nlogs)),
        "url": "/routes/{}".format(os.path.basename(os.path.dirname(json_path))),
    }
    with open(json_path, "w", encoding="utf8") as f:
        json.dump(doc, f)


def write_stream(video_file, route_dir):
    """Generate the HLS video playlist stream."""
    playlist_path = os.path.join(route_dir, "video.m3u8")

    if os.path.exists(playlist_path):
        print(
            f"Skipping transcoding {video_file} -> {playlist_path}. "
            "Destination file exists. Delete to force transcode."
        )
    else:
        print(f"Transcoding {video_file}...")
        # Note: these options assume VAAPI accelerated h264 encoding is available & configured
        cmd = [
            "ffmpeg",
            "-hwaccel",
            "vaapi",
            "-hwaccel_output_format",
            "vaapi",
            "-i",
            video_file,
            "-c:v",
            "h264_vaapi",
            "-b:v",
            "500k",
            "-vf",
            "scale_vaapi=w=526:h=330",
            "-c:a",
            "copy",
            "-r",
            "20",
            "-hls_time",
            "60",
            "-hls_list_size",
            "0",
            "-hls_allow_cache",
            "1",
            playlist_path,
        ]

        res = subprocess.run(cmd, capture_output=True)
        if res.returncode == 0:
            print("Transcoding finished.")
        else:
            print(f"Transcoding failed. ffmpeg return code {res.returncode}")
            print(f'Command line: {" ".join(cmd)}')
            print(repr(res.stdout))
            print(repr(res.stderr))
            if os.path.exists(playlist_path):
                os.unlink(playlist_path)
            raise SystemExit(1)


def make_index_file(routes_dir):
    """Write a routes.json index file with a list of route names."""
    route_names = [
        os.path.basename(os.path.dirname(r))
        for r in glob.glob(os.path.join(routes_dir, "*/route.json"))
    ]

    with open(os.path.join(routes_dir, "routes.json"), "w") as f:
        json.dump(route_names, f)
    print(f"Found {len(route_names)} routes in directory")


def main():  # noqa: D103
    yaml_path = sys.argv[1]
    yaml_dir = os.path.dirname(yaml_path)

    routes_dir = sys.argv[2]

    with open(yaml_path) as yamlfile:
        logs = yaml.load(yamlfile, yaml.Loader)

    for log in logs:
        logfile = os.path.normpath(os.path.join(yaml_dir, log["logfile"]))
        video = os.path.normpath(os.path.join(yaml_dir, log["video"]))
        print(log["sync"])
        sync = (log["sync"]["video_s"], log["sync"]["log_us"])
        make_route((log["car"], log["car_details"]), logfile, video, sync, routes_dir)

    make_index_file(routes_dir)


if __name__ == "__main__":
    main()

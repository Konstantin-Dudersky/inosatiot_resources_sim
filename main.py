import datetime
import getopt
import sys
import time

import enlighten
import yaml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from loguru import logger

import electricity

logger.remove()
logger.add(sys.stderr, level='INFO')
logger.add('logs/log.txt', level='INFO', rotation='5 MB')


def check_bucket(client: InfluxDBClient, bucket_name: str):
    try:
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
    except Exception as e:
        logger.critical(e)
        sys.exit(1)

    if bucket is None:
        logger.warning(f"bucket {bucket_name} in host {client.url} not found")
        bucket = client.buckets_api().create_bucket(bucket_name=bucket_name)
        logger.success(f"bucket {bucket.name} in host {client.url} created")


if __name__ == "__main__":

    opts, args = getopt.getopt(sys.argv[1:], "hm:b:", ['help', 'mode=', 'start=', 'stop=', 'period=', 'bsize='])

    mode = None
    start = None
    stop = None
    period = 10
    bsize = 1000

    for opt, arg in opts:

        if opt in ['-h', '--help']:
            print(f"""
Simulator

arguments:
    -h, --help : show help
    --mode (required) : mode
        rt - cyclical execute in real-time
        batch - wtite batch data in range (--start, --stop)
    --start <datetime in isoformat> (required for batch mode):
        start timestamp
    --stop <datetime in isoformat> (optional for batch mode):
        stop timestamp. If not specified, the current time is used.
    --period (optional):
        Time in seconds between sequential points. If not specified, {period} is used.
    --bsize (optional):
        Number of measurements in one packet. Default {bsize}
    
examples:
    venv/bin/python3 main.py --mode rt
    venv/bin/python3 main.py --mode batch --start 2021-01-01T00:00:00+03:00
    venv/bin/python3 main.py --mode batch --start 2021-01-01T00:00:00+03:00 --stop 2022-01-01T00:00:00+03:00 \\r
    --period {period} --bsize {bsize}
""")
            sys.exit()
        elif opt == '--mode':
            if arg in ['rt', 'batch']:
                mode = arg

        elif opt == '--start':
            try:
                start = datetime.datetime.fromisoformat(arg)
            except ValueError as ve:
                print(f'Invalid --start value:\n{ve}')
                sys.exit()

        elif opt == '--stop':
            try:
                stop = datetime.datetime.fromisoformat(arg)
            except ValueError as ve:
                print(f'Invalid --stop value:\n{ve}')
                sys.exit()

        elif opt == '--period':
            period = int(arg)

        elif opt == '--bsize':
            bsize = int(arg)

    if mode is None:
        print(f"Invalid --mode value, see --help")
        sys.exit()
    elif mode == 'rt':
        print('Start rt mode, press CTRL-C for exit')
    elif mode == 'batch':
        if start is None:
            print(f"Value --start required for batch mode, exit")
            sys.exit()
        if stop is None:
            stop = datetime.datetime.now().astimezone()

        if start.tzname() is None:
            local_tz = datetime.datetime.now().astimezone().tzinfo
            start = start.astimezone(local_tz)

        if stop.tzname() is None:
            local_tz = datetime.datetime.now().astimezone().tzinfo
            stop = stop.astimezone(local_tz)

        print(f"Start batch mode, start:{start.isoformat()}, stop:{stop.isoformat()}")

    batch_ts = start

    with open('../config_inosatiot_resources_sim.yaml') as stream:
        config = yaml.safe_load(stream)

    TOKEN = config['influxdb']['token']
    ORG = config['influxdb']['org']
    URL = config['influxdb']['url']
    BUCKET = config['influxdb']['bucket']

    client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    check_bucket(client, BUCKET)

    ts = datetime.datetime.now().astimezone().astimezone(datetime.datetime.now().astimezone().tzinfo)

    # electricity
    energymeters = {}
    for e in config['electricity']:
        label = e['label']
        if label not in energymeters:
            energymeters[label] = electricity.SimElectricity(
                label=label,
                now=ts if mode == 'rt' else batch_ts,
                i=e['i'],
                v=e['v'],
                pf=e['pf'],
                f=e['f'],
                q_ind=e['q_ind'],
            )

    if mode == 'rt':
        while True:
            ts = datetime.datetime.now().astimezone().astimezone(datetime.datetime.now().astimezone().tzinfo)

            record = []
            for key in energymeters:
                record.extend(energymeters[key].cycle(ts))

            write_api.write(
                bucket=BUCKET,
                record=record
            )

            time.sleep(period)
    elif mode == 'batch':
        progress_bar = enlighten.Counter(
            total=int((stop - start).total_seconds()),
            desc=f"{start.isoformat()} | {stop.isoformat()} |",
            unit='hours')

        progress_bar.update(0)

        while True:

            record = []
            for i in range(bsize):
                for key in energymeters:
                    record.extend(energymeters[key].cycle(batch_ts))

                batch_ts += datetime.timedelta(seconds=period)
                progress_bar.update(int(datetime.timedelta(seconds=period).total_seconds()))

                if batch_ts >= stop:
                    break

            write_api.write(
                bucket=BUCKET,
                record=record
            )

            if batch_ts >= stop:

                print("\nBatch execution finished")
                sys.exit()

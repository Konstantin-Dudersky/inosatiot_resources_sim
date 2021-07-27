import datetime
import getopt
import sys
import time

import yaml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

import electricity


def progress_bar(current, total, bar_length=20):
    percent = float(current) * 100 / total
    arrow = '-' * int(percent / 100 * bar_length - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')


if __name__ == "__main__":

    opts, args = getopt.getopt(sys.argv[1:], "hm:b:", ['help', 'mode=', 'start=', 'stop='])

    mode = None
    start = None
    stop = None

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
    --stop <datetime in isoformat> (optional for batch mode):
    
examples:
    venv/bin/python3 main.py --mode rt
    venv/bin/python3 main.py --mode batch --start 2021-01-01T00:00:00+03:00
    venv/bin/python3 main.py --mode batch --start 2021-01-01T00:00:00+03:00 --stop 2022-01-01T00:00:00+03:00
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

    # sys.exit()

    with open('../config_inosatiot_resources_sim.yaml') as stream:
        config = yaml.safe_load(stream)

    TOKEN = config['influxdb']['token']
    ORG = config['influxdb']['org']
    URL = config['influxdb']['url']
    BUCKET = config['influxdb']['bucket']

    client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    ts = datetime.datetime.now().astimezone().astimezone(datetime.datetime.now().astimezone().tzinfo)

    # electricity
    energymeters = {}
    for e in config['electricity']:
        label = e['label']
        if label not in energymeters:
            energymeters[label] = electricity.SimElectricity(
                label=label,
                p_base=e['p']['base'], p_var=e['p']['var'], p_delay=e['p']['delay'],
                q_base=e['q']['base'], q_var=e['p']['var'], q_delay=e['p']['delay'],
                now=ts if mode == 'rt' else batch_ts
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

            time.sleep(10)
    elif mode == 'batch':
        while True:
            progress_bar((batch_ts - start).total_seconds(), (stop - start).total_seconds(), bar_length=50)

            record = []
            for i in range(10000):
                for key in energymeters:
                    record.extend(energymeters[key].cycle(batch_ts))

                batch_ts += datetime.timedelta(seconds=10)
                if batch_ts >= stop:
                    break

            write_api.write(
                bucket=BUCKET,
                record=record
            )

            if batch_ts >= stop:
                progress_bar((batch_ts - start).total_seconds(), (stop - start).total_seconds(), bar_length=50)

                print("\nBatch execution finished")
                sys.exit()

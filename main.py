import time

import yaml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

import electricity

with open('../config_inosatiot_resources_sim.yaml') as stream:
    config = yaml.safe_load(stream)

TOKEN = config['influxdb']['token']
ORG = config['influxdb']['org']
URL = config['influxdb']['url']
BUCKET = config['influxdb']['bucket']

client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# electricity

energymeters = {}
for e in config['electricity']:
    label = e['label']
    if label not in energymeters:
        energymeters[label] = electricity.SimElectricity(
            label=label,
            p_base=e['p']['base'], p_var=e['p']['var'], p_delay=e['p']['delay'],
            q_base=e['q']['base'], q_var=e['p']['var'], q_delay=e['p']['delay'],
        )

while True:

    record = []
    for key in energymeters:
        record.extend(energymeters[key].cycle())

    write_api.write(
        bucket=BUCKET,
        record=record
    )

    time.sleep(10)

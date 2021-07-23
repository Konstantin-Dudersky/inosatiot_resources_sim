import time

import yaml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

import energymeter

with open('../config_inosatiot_resources_sim.yaml') as stream:
    data = yaml.safe_load(stream)

TOKEN = data['influxdb']['token']
ORG = data['influxdb']['org']
URL = data['influxdb']['url']
BUCKET = data['influxdb']['bucket']

client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

energymeters = {}

while True:
    with open('../config_inosatiot_resources_sim.yaml') as stream:
        data = yaml.safe_load(stream)

    # electricity
    for em in data['electricity']:
        name = em['name']
        if name not in energymeters:
            energymeters[name] = energymeter.SimMachine(name)

        energymeters[name].ep_imp_increment = int(em['ep_imp_increment'])
        energymeters[name].ep_exp_increment = int(em['ep_exp_increment'])
        energymeters[name].eq_imp_increment = int(em['eq_imp_increment'])
        energymeters[name].eq_exp_increment = int(em['eq_exp_increment'])

    record = []

    for key in energymeters:
        record.extend(energymeters[key].cycle())

    write_api.write(bucket=BUCKET,
                    record=record)

    time.sleep(10)

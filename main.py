import datetime
import struct
import time

from influxdb import InfluxDBClient
import serial

# configure the serial connections (the parameters differs on the device you are connecting to)
ser = serial.Serial(
    port='/dev/ttyUSB0',
    baudrate=9600,
    parity=serial.PARITY_EVEN,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=5)

COMMANDS = {
    "L1_VOLTAGE": "01 03 00 0E 00 02 A5 C8",
    "L2_VOLTAGE": "01 03 00 10 00 02 C5 CE",
    "L3_VOLTAGE": "01 03 00 12 00 02 64 0E",

    "GRID_FREQUENCY": "01 03 00 14 00 02 84 0F",

    "L1_CURRENT": "01 03 00 16 00 02 25 CF",
    "L2_CURRENT": "01 03 00 18 00 02 44 0C",
    "L3_CURRENT": "01 03 00 1A 00 02 E5 CC",

    # "TOTAL_ACTIVE_POWER": "01 03 00 1C 00 02 05 CD",
    "L1_ACTIVE_POWER": "01 03 00 1E 00 02 A4 0D",
    "L2_ACTIVE_POWER": "01 03 00 20 00 02 C5 C1",
    "L3_ACTIVE_POWER": "01 03 00 22 00 02 64 01",

    # "TOTAL_REACTIVE_POWER": "01 03 00 24 00 02 84",
    "L1_REACTIVE_POWER": "01 03 00 26 00 02 25 C0",
    "L2_REACTIVE_POWER": "01 03 00 28 00 02 44 03",
    "L3_REACTIVE_POWER": "01 03 00 2A 00 02 E5 C3",

    # "TOTAL_APPARENT_POWER": "01 03 00 2C 00 02 05 C2",
    "L1_APPARENTPOWER": "01 03 00 2E 00 02 A4 02",
    "L2_APPARENT_POWER": "01 03 00 30 00 02 C4 04",
    "L3_APPARENT_POWER": "01 03 00 32 00 02 65 C4",

    # "TOTAL_POWER_FACTOR": "01 03 00 34 00 02 85 C5",
    "L1_POWER_FACTOR": "01 03 00 36 00 02 24 05",
    "L2_POWER_FACTOR": "01 03 00 38 00 02 45 C6",
    "L3_POWER_FACTOR": "01 03 00 3A 00 02 E4 06",

    # "TOTAL_ACTIVE_ENERGY": "01 03 01 00 00 02 C5 F7",
    "L1_TOTAL_ACTIVE_ENERGY": "01 03 01 02 00 02 64 37",
    "L2_TOTAL_ACTIVE_ENERGY": "01 03 01 04 00 02 84 36",
    "L3_TOTAL_ACTIVE_ENERGY": "01 03 01 06 00 02 25 F6",

    # "FORWARD_ACTIVE_ENERGY": "01 03 01 08 00 02 44 35",
    "L1_FORWARD_ACTIVE_ENERGY": "01 03 01 0A 00 02 E5 F5",
    "L2_FORWARD_ACTIVE_ENERGY": "01 03 01 0C 00 02 05 F4",
    "L3_FORWARD_ACTIVE_ENERGY": "01 03 01 0E 00 02 A4 34",

    # "REVERSE_ACTIVE_ENERGY": "01 03 01 10 00 02 C4 32",
    "L1_REVERSE_ACTIVE_ENERGY": "01 03 01 12 00 02 65 F2",
    "L2_REVERSE_ACTIVE_ENERGY": "01 03 01 14 00 02 85 F3",
    "L3_REVERSE_ACTIVE_ENERGY": "01 03 01 16 00 02 24 33",

    # "TOTAL_REACTIVE_ENERGY": "01 03 01 18 00 02 45 F0",
    "L1_TOTAL_REACTIVE_ENERGY": "01 03 01 1A 00 02 E4 30",
    "L2_TOTAL_REACTIVE_ENERGY": "01 03 01 1C 00 02 04 31",
    "L3_TOTAL_REACTIVE_ENERGY": "01 03 01 1E 00 02 A5 F1",

    # "FORWARD_REACTIVE_ENERGY": "01 03 01 20 00 02 C4 3D",
    "L1_FORWARD_REACTIVE_ENERGY": "01 03 01 22 00 02 65 FD",
    "L2_FORWARD_REACTIVE_ENERGY": "01 03 01 24 00 02 85 FC",
    "L3_FORWARD_REACTIVE_ENERGY": "01 03 01 26 00 02 24 3C",

    # "REVERSE_REACTIVE_ENERGY": "01 03 01 28 00 02 45 FF",
    "L1_REVERSE_REACTIVE_ENERGY": "01 03 01 2A 00 02 E4 3F",
    "L2_REVERSE_REACTIVE_ENERGY": "01 03 01 2C 00 02 04 3E",
    "L3_REVERSE_REACTIVE_ENERGY": "01 03 01 2E 00 02 A5 FE",

    # "T1_TOTAL_ACTIVE_ENERGY": "01 03 01 30 00 02 C5 F8",
    # "T1_FORWARD_ACTIVE_ENERGY": "01 03 01 32 00 02 64 38",
    # "T1_REVERSE_ACTIVE_ENERGY": "01 03 01 34 00 02 84 39",
    # "T1_TOTAL_REACTIVE_ENERGY": "01 03 01 36 00 02 25 F9",
    # "T1_FORWARD_REACTIVE_ENERGY": "01 03 01 38 00 02 44 3A",
    # "T1_REVERSE_REACTIVE_ENERGY": "01 03 01 3A 00 02 E5 FA",
    #
    # "T2_TOTAL_ACTIVE_ENERGY": "01 03 01 3C 00 02 05 FB",
    # "T2_FORWARD_ACTIVE_ENERGY": "01 03 01 3E 00 02 A4 3B",
    # "T2_REVERSE_ACTIVE_ENERGY": "01 03 01 40 00 02 C4 23",
    # "T2_TOTAL_REACTIVE_ENERGY": "01 03 01 42 00 02 65 E3",
    # "T2_FORWARD_REACTIVE_ENERGY": "01 03 01 44 00 02 85 E2",
    # "T2_REVERSE_REACTIVE_ENERGY": "01 03 01 46 00 02 24 22",
    #
    # "T3_TOTAL_ACTIVE_ENERGY": "01 03 01 48 00 02 45 E1",
    # "T3_FORWARD_ACTIVE_ENERGY": "01 03 01 4A 00 02 E4 21",
    # "T3_REVERSE_ACTIVE_ENERGY": "01 03 01 4C 00 02 04 20",
    # "T3_TOTAL_REACTIVE_ENERGY": "01 03 01 4E 00 02 A5 E0",
    # "T3_FORWARD_REACTIVE_ENERGY": "01 03 01 50 00 02 C5 E6",
    # "T3_REVERSE_REACTIVE_ENERGY": "01 03 01 52 00 02 64 26",
    #
    # "T4_TOTAL_ACTIVE_ENERGY": "01 03 01 54 00 02 84 27",
    # "T4_FORWARD_ACTIVE_ENERGY": "01 03 01 56 00 02 25 E7",
    # "T4_REVERSE_ACTIVE_ENERGY": "01 03 01 58 00 02 44 24",
    # "T4_TOTAL_REACTIVE_ENERGY": "01 03 01 5A 00 02 E5 E4",
    # "T4_FORWARD_REACTIVE_ENERGY": "01 03 01 5C 00 02 05 E5",
    # "T4_REVERSE_REACTIVE_ENERGY": "01 03 01 5E 00 02 A4 25",
}


def get_value(msg):
    for msg_byte in msg.split(" "):
        send_byte(bytearray.fromhex(msg_byte))
    response = ser.read(9)
    response_data = response[3:7]
    return struct.unpack('>f', response_data)[0]


def send_byte(data):
    ser.write(data)
    time.sleep(0.01)


PERIOD = 60

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'energy_meter')
client.create_database('energy_meter')

while True:
    for (measurement, command) in COMMANDS.items():
        print("Reading %s..." % measurement)
        value = get_value(command)
        print("%s = %s" % (measurement, value))
        time.sleep(0.25)

        json_body = [
            {
                "measurement": measurement,
                "tags": {
                },
                "time": datetime.datetime.now().isoformat(),
                "fields": {
                    "value": value
                }
            }]
        client.write_points(json_body)
    time.sleep(PERIOD)
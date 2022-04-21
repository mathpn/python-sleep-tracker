"""
Script to stream sensor data from MetaWear device and store in a CSV file.
Provide the MAC address of the MetaWear device as the --address argument.
"""

import argparse
import multiprocessing as mp
from threading import Event

from mbientlab.metawear import MetaWear

from src.data_writer import DataWriter
from src.helpers import consume_data_queue
from src.logger import LOG
from src.sensor_stream import SensorDevice


def main():
    parser = argparse.ArgumentParser(description='Stream sensor data from MetaWear device.')
    parser.add_argument('--address', type=str, help='MetaWear device MAC address.', required=True)
    parser.add_argument('--raw', action='store_true', help='Stream raw sensor data.')
    parser.add_argument('--max-queue-size', type=int, help='Maximum queue size.', default=2500)
    args = parser.parse_args()

    device = MetaWear(args.address)

    acc_queue = mp.Queue(args.max_queue_size)
    gyro_queue = mp.Queue(args.max_queue_size)
    acc_queue_event = mp.Event()
    gyro_queue_event = mp.Event()

    main_event = Event()
    acc_writer = DataWriter("data/acc_data.csv", "(g/s)", raw=args.raw)
    gyro_writer = DataWriter("data/gyro_data.csv", "(rad/s)", raw=args.raw)
    stream = SensorDevice(device, acc_queue, gyro_queue, raw=args.raw)
    stream.connect()
    stream.register_sensors(50.0, 8.0)

    stream_process = mp.Process(target=stream.stream_data())
    acc_write_process = mp.Process(target=consume_data_queue, args=(acc_queue_event, acc_queue, acc_writer))
    gyro_write_process = mp.Process(target=consume_data_queue, args=(gyro_queue_event, gyro_queue, gyro_writer))
    stream_process.start()
    acc_write_process.start()
    gyro_write_process.start()
    try:
        main_event.wait()
    except KeyboardInterrupt:
        print()
        LOG.info('Keyboard interrupt received. Stopping stream.')
        if stream_process.is_alive():
            stream_process.close()
        stream.stop_streaming()
        LOG.info("waiting for write process to finish, please wait...")
        acc_queue.close()
        gyro_queue.close()
        acc_queue_event.wait(5)
        gyro_queue_event.wait(5)
        acc_write_process.kill()
        gyro_write_process.kill()
        acc_writer.close()
        gyro_writer.close()
        LOG.info("exiting")
        main_event.set()


if __name__ == "__main__":
    main()

"""
Miscelaneous helper functions.
"""

import queue
import signal
import multiprocessing as mp
from multiprocessing.synchronize import Event
import threading
import time

from src.data_writer import DataWriter
from src.device import MetaWearDevice
from src.logger import LOG
from src.streamer import DataStreamer


def disable_control_c(func):
    """ Decorator to disable KeyboardInterrupt exit, use with caution. """

    def wrapper(*args, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)

    return wrapper


@disable_control_c
def consume_data_queue(event: Event, data_queue: mp.Queue, writer: DataWriter, sleep_time: int = 2) -> None:
    """ Consume a data queue with a writer."""
    while True:
        try:
            data = data_queue.get(timeout=1)
            event.clear()
            writer.write_data(data)
        except queue.Empty:
            LOG.debug("No data in queue")
            event.set()
            time.sleep(sleep_time)


def stream_data(
        device: MetaWearDevice, session_id: int, acc_max_queue_size: int, gyro_max_queue_size: int,
        raw: bool = False) -> None:
    
    if not device.connected:
        device.connect()

    acc_queue = mp.Queue(acc_max_queue_size)
    gyro_queue = mp.Queue(gyro_max_queue_size)
    acc_queue_event = mp.Event()
    gyro_queue_event = mp.Event()

    main_event = threading.Event()
    acc_writer = DataWriter(f"data/session/{session_id}_acc_data.csv", "(g/s)", raw=raw)
    gyro_writer = DataWriter(f"data/sessions/{session_id}_gyro_data.csv", "(rad/s)", raw=raw)
    streamer = DataStreamer(device, acc_queue, gyro_queue, raw=raw)
    streamer.subscribe_to_sensors()

    stream_process = mp.Process(target=streamer.start_streaming())
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
        streamer.stop_streaming()
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

"""
Miscelaneous helper functions.
"""

import queue
import signal
from multiprocessing import Queue
from multiprocessing.synchronize import Event
import time

from src.data_writer import DataWriter


def disable_control_c(func):
    """ Decorator to disable KeyboardInterrupt exit, use with caution. """

    def wrapper(*args, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)

    return wrapper


@disable_control_c
def consume_data_queue(event: Event, data_queue: Queue, writer: DataWriter, sleep_time: int = 2) -> None:
    """ Consume a data queue with a writer."""
    while True:
        try:
            data = data_queue.get(timeout=1)
            event.clear()
            writer.write_data(data)
        except queue.Empty:
            print("No data in queue")
            event.set()
            time.sleep(sleep_time)

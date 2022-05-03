"""
Module containing helper functions and DataStreamer class for streaming sensor data
from a Device abstract class instance.

A Device subclass must implement the following methods:
    - start_streaming()
    - stop_streaming()
    - subscribe_to_sensors(data_processor_creator: Optional[Callable])
"""

from functools import partial
import multiprocessing as mp

from mbientlab.metawear import parse_value

from src.device import Device
from src.logger import LOG


class DataStreamer:
    """
    Simple class to live-stream sensor data from a bluetooth device.


    """
    def __init__(self, device: Device, acc_queue: mp.Queue, gyro_queue: mp.Queue, raw: bool = False):
        """
        Parameters:
            device (MetaWear): MetaWear device object.
            acc_queue (mp.Queue): Queue to store accelerometer data.
            gyro_queue (mp.Queue): Queue to store gyroscope data.
            raw (bool): If True, raw sensor data is streamed.
        """
        self.device = device
        self.samples = 0
        self.acc_queue = acc_queue
        self.gyro_queue = gyro_queue
        self.raw = raw

    def _acc_data_handler(self):
        return partial(self._data_handler, data_queue=self.acc_queue, msg='acc')

    def _gyro_data_handler(self):
        return partial(self._data_handler, data_queue=self.gyro_queue, msg='gyro')

    def _data_handler(self, _, raw_data, data_queue: mp.Queue, msg: str) -> None:
        data = parse_value(raw_data)
        LOG.debug(f'{msg}: {data:.4f}')
        if self.raw:
            data = [data.x, data.y, data.z]
        else:
            data = [data]
        data_queue.put(data)
        self.samples += 1

    def start_streaming(self) -> None:
        """ Stream sensor data. """
        self.device.start_streaming()
        LOG.info("Streaming data...")

    def stop_streaming(self) -> None:
        """ Stop streaming data. """
        LOG.info("Stopping streaming...")
        self.device.stop_streaming()
        LOG.info(f"streamed {self.samples} samples")
        self.samples = 0

    def subscribe_to_sensors(self) -> None:
        """ Subscribe to sensor signals. """
        self.device.subscribe_to_accelerometer(self._acc_data_handler())
        self.device.subscribe_to_gyroscope(self._gyro_data_handler())

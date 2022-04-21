import signal
import sys
import time
import multiprocessing as mp
import queue
from threading import Event
from typing import Tuple

from mbientlab.metawear import cbindings, libmetawear, MetaWear, parse_value
from mbientlab import warble


def disable_control_c(func):
    """ Decorator to disable KeyboardInterrupt exit, use with caution. """

    def wrapper(*args, **kwargs):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)

    return wrapper


class DataWriter:
    """ Class used to write sensor data to a file. """

    def __init__(self, filename: str, units: str):
        self.filename = filename
        self.units = units
        self.file = open(f"{self.filename}", "w", encoding="utf-8")
        self._write_header()

    def _write_header(self):
        """ Write the header to the file. """
        self.file.write(f"timestamp,x {self.units},y {self.units},z {self.units}\n")

    def _format_line(self, data: Tuple[float, float, float]) -> str:
        """ Format float data to string. """

        def parse_floats(x):
            return f"{x:.4f}"

        return f"{time.time():.3f},{','.join(map(parse_floats, data))}\n"

    def write_data(self, data: Tuple[float, float, float]) -> None:
        """ Write sensor data to file. """
        self.file.write(self._format_line(data))

    def close(self) -> None:
        """ Close the file. """
        self.file.close()


class StreamDevice:
    def __init__(self, device, acc_queue: mp.Queue, gyro_queue: mp.Queue):
        self.device = device
        self.samples = 0
        self.acc_queue = acc_queue
        self.gyro_queue = gyro_queue
        self.acc_callback = cbindings.FnVoid_VoidP_DataP(self._acc_data_handler)
        self.gyro_callback = cbindings.FnVoid_VoidP_DataP(self._gyro_data_handler)
        self.acc = False
        self.gyro = False
        self.temperatue = False

    def _acc_data_handler(self, _, raw_data):
        data: cbindings.CartesianFloat = parse_value(raw_data)
        data_tuple = (data.x, data.y, data.z)
        self.acc_queue.put(data_tuple)
        # self.stream_writer.write_acc_data(data_tuple)
        # self.samples += 1

    def _gyro_data_handler(self, _, raw_data):
        data: cbindings.CartesianFloat = parse_value(raw_data)
        data_tuple = (data.x, data.y, data.z)
        self.gyro_queue.put(data_tuple)
        # self.stream_writer.write_gyro_data(data_tuple)
        # self.samples += 1

    def connect(
        self, min_conn_interval: float = 7.5, max_conn_interval: float = 7.5, latency: int = 0, timeout: int = 6000
    ) -> None:
        self.device.on_disconnect = lambda status: print("disconnected")
        self.device.connect()
        time.sleep(1)
        if not self.device.is_connected:
            raise ConnectionError("Failed to connect to MetaWear board")
        libmetawear.mbl_mw_settings_set_connection_parameters(
            self.device.board, min_conn_interval, max_conn_interval, latency, timeout
        )
        time.sleep(1)

    def stream_data(self) -> None:
        """ Stream sensor data. """
        if self.acc:
            libmetawear.mbl_mw_acc_enable_acceleration_sampling(self.device.board)
        if self.gyro:
            libmetawear.mbl_mw_gyro_bmi160_enable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")
        print("Streaming data...")
        libmetawear.mbl_mw_acc_start(self.device.board)
        libmetawear.mbl_mw_gyro_bmi160_start(self.device.board)

    def stop_streaming(self) -> None:
        """ Stop streaming data. """
        print("Stopping streaming...")
        if self.acc:
            print("removing acc callback")
            libmetawear.mbl_mw_acc_stop(self.device.board)
            libmetawear.mbl_mw_acc_disable_acceleration_sampling(self.device.board)
        if self.gyro:
            print("removing gyro callback")
            libmetawear.mbl_mw_gyro_bmi160_stop(self.device.board)
            libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")
        time.sleep(1)
        print(f"streamed {self.samples} samples")
        self.samples = 0

    def register_sensors(self, frequency: float, acc_range: float = 8.0) -> None:
        acc_signal = self._register_accelerometer(frequency, acc_range)
        gyro_signal = self._register_gyroscope()
        self._subscribe_sensors(acc_signal, gyro_signal)

    def _register_accelerometer(self, frequency: float, value_range: float) -> int:
        """ Register the accelerometer data signal. """
        libmetawear.mbl_mw_acc_set_odr(self.device.board, frequency)
        libmetawear.mbl_mw_acc_set_range(self.device.board, value_range)
        libmetawear.mbl_mw_acc_write_acceleration_config(self.device.board)
        signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(self.device.board)
        self.acc = True
        return signal

    def _register_gyroscope(self) -> int:
        """ Register the gyroscope data signal """
        libmetawear.mbl_mw_gyro_bmi160_set_odr(self.device.board, cbindings.GyroBoschOdr._50Hz)
        libmetawear.mbl_mw_gyro_bmi160_set_range(self.device.board, cbindings.GyroBoschRange._1000dps)
        libmetawear.mbl_mw_gyro_bmi160_write_config(self.device.board)
        signal = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(self.device.board)
        self.gyro = True
        return signal

    def _subscribe_sensors(self, acc_signal, gyro_signal):
        libmetawear.mbl_mw_datasignal_subscribe(acc_signal, None, self.acc_callback)
        libmetawear.mbl_mw_datasignal_subscribe(gyro_signal, None, self.gyro_callback)


@disable_control_c
def consume_data_queue(event: Event, data_queue: mp.Queue, writer: DataWriter, sleep_time: int = 2) -> None:
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


def main():
    address = sys.argv[1]
    device = MetaWear(address)

    acc_queue = mp.Queue(25000)
    gyro_queue = mp.Queue(25000)
    acc_queue_event = mp.Event()
    gyro_queue_event = mp.Event()

    main_event = Event()
    acc_writer = DataWriter("acc_data.csv", "(g/s)")
    gyro_writer = DataWriter("gyro_data.csv", "(rad/s)")
    stream = StreamDevice(device, acc_queue, gyro_queue)
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
        if stream_process.is_alive():
            stream_process.close()
        stream.stop_streaming()
        print("waiting for write process to finish, please wait...")
        acc_queue.close()
        gyro_queue.close()
        acc_queue_event.wait(5)
        gyro_queue_event.wait(5)
        acc_write_process.kill()
        gyro_write_process.kill()
        acc_writer.close()
        gyro_writer.close()
        print("exiting")
        main_event.set()


if __name__ == "__main__":
    main()

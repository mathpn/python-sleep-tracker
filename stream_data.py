import sys
import time

from mbientlab.metawear import MetaWear, parse_value, libmetawear
from mbientlab.metawear.cbindings import *
from mbientlab.warble import *


# FIXME segmentation fault (core dumped) sometimes when streaming

class SensorStream:
    """ Class used to stream sensor data from the MetaWear board. """

    def __init__(self, device):
        self.device = device
        self.samples = 0
        self.acc = False
        self.gyro = False
        self.temperatue = False

    def connect(self, min_conn_interval: float = 7.5, max_conn_interval: float = 7.5, latency: int = 0, timeout: int = 6000):
        self.device.on_disconnect = lambda status: print("disconnected")
        self.device.connect()
        time.sleep(1)
        if not self.device.is_connected:
            raise ConnectionError("Failed to connect to MetaWear board")
        libmetawear.mbl_mw_settings_set_connection_parameters(
            self.device.board, min_conn_interval, max_conn_interval, latency, timeout)
        time.sleep(1.5)

    def stream_data(self):
        """ Stream sensor data. """
        if self.acc:
            libmetawear.mbl_mw_acc_enable_acceleration_sampling(self.device.board)
        if self.gyro:
            libmetawear.mbl_mw_gyro_enable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")

        print("Streaming data...")
        libmetawear.mbl_mw_acc_start(self.device.board)

    def stop_streaming(self):
        """ Stop streaming data. """
        if self.acc:
            libmetawear.mbl_mw_acc_stop(self.device.board)
            libmetawear.mbl_mw_acc_disable_acceleration_sampling(self.device.board)
        if self.gyro:
            libmetawear.mbl_mw_gyro_stop(self.device.board)
            libmetawear.mbl_mw_gyro_disable_rotation_sampling(self.device.board)
        if self.temperatue:
            raise NotImplementedError("Temperature streaming is not yet implemented")

    def register_accelerometer(self, frequency: float, value_range: float):
        """ Register the accelerometer data signal. """
        libmetawear.mbl_mw_acc_set_odr(self.device.board, frequency)
        libmetawear.mbl_mw_acc_set_range(self.device.board, value_range)
        libmetawear.mbl_mw_acc_write_acceleration_config(self.device.board)
        signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(self.device.board)
        register_callback(signal, self._data_handler)
        self.acc = True

    def _data_handler(self, ctx, data):
        # TODO proper data handling to storage
        print("%s -> %s" % (self.device.address, parse_value(data)))
        self.samples += 1


def main():
    address = sys.argv[1]
    device = MetaWear(address)

    stream = SensorStream(device)
    stream.connect()
    stream.register_accelerometer(25.0, 16.0)
    stream.stream_data()
    time.sleep(10)
    stream.stop_streaming()


if __name__ == "__main__":
    main()

from enum import Enum
import os
import time
from typing import Dict
from src.device import MetaWearDevice
from src.metadata import MetadataDB


class CLI:

    def __init__(self, data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, 'sessions'), exist_ok=True)
        self.metadata_db = MetadataDB(f'{data_dir}/metadata.db')

    def _lookup_device(self, mac_address: str) -> Dict:
        device = self.metadata_db.lookup_device(mac_address)
        return device

    def log_in(self) -> Dict:
        username = input("Please provide a username: ")
        user_id = self.metadata_db.get_user_id(username)
        print(f"Welcome {username}")
        device_config = self._choose_device(user_id)
        self.metadata_db.add_user_device(user_id, device_config['device_id'])
        device = MetaWearDevice(device_config['mac_address'])
        _configure_device(device, device_config)
        device_id = device_config.pop('device_id')
        user_data = {
            "user_id": user_id, "device_id": device_id, "device_config": device_config, "device": device
        }
        return user_data

    def start_session(self, user_id: int, device_id: int) -> int:
        timestamp = int(time.time())
        session_id = self.metadata_db.add_session(user_id, timestamp, device_id)
        print(f'Session ID {session_id} started')
        return session_id

    def _choose_device(self, user_id: int) -> Dict:
        devices = self.metadata_db.get_user_devices(user_id)
        if devices:
            print('You have the following device configurations saved:')
            for i, device_config in enumerate(devices):
                print(f'Device {i+1}: \n{dict_to_str(device_config)}')
            use_device = input("Would you like to use one of them? (y/n) ")
            if use_device in 'Yy':
                device_id = 0
                while device_id not in range(1, len(devices)+1):
                    device_id = int(input("Please provide the device ID: "))
                print(f'Using device {device_id}')
                return devices[device_id-1]

        device_config = {}
        while not device_config:
            mac_address = input("Please provide a MAC address: ")
            device_config = _input_device_config(mac_address)
            if not device_config:
                print("Invalid configuration, please try again")
        if device_config not in devices:
            device_id = self._save_device_config(device_config)
            device_config['device_id'] = device_id
            print(f'Device ID {device_id} saved')
        else:
            device_config = devices[devices.index(device_config)]
        return device_config

    def _save_device_config(self, device_config: Dict) -> int:
        device_id = self.metadata_db.add_device(device_config)
        return device_id


def _input_device_config(mac_address: str) -> Dict:
    device = MetaWearDevice(mac_address)
    device_options = _get_device_options(device)
    acc_freq_options, gyro_freq_options, acc_range_options, gyro_range_options = device_options.values()
    print(f'{mac_address} has the following options:')
    print(f'Accelerometer Frequency: \n{enum_to_str(acc_freq_options)}')
    acc_freq = input(
        "Please choose the index of the frequency you would like to use (recommended: at least 25Hz): ")
    try:
        acc_freq_options(int(acc_freq))
    except ValueError as exc:
        print(f'{exc}')
        return {}
    print(f'Accelerometer Range: \n{enum_to_str(acc_range_options)}')
    acc_range = input(
        "Please choose the index of the range you would like to use (recommended: 8g): ")
    try:
        acc_range_options(int(acc_range))
    except ValueError as exc:
        print(f'{exc}')
        return {}
    print(f'Gyroscope Frequency: \n{enum_to_str(gyro_freq_options)}')
    gyro_freq = input(
        "Please choose the index of the frequency you would like to use (recommended: at least 50Hz): ")
    try:
        gyro_freq_options(int(gyro_freq))
    except ValueError as exc:
        print(f'{exc}')
        return {}
    print(f'Gyroscope Range: \n{enum_to_str(gyro_range_options)}')
    gyro_range = input(
        "Please choose the index of the range you would like to use (reccommended: 1000dps): ")
    try:
        gyro_range_options(int(gyro_range))
    except ValueError as exc:
        print(f'{exc}')
        return {}
    raw = input("Would you like to use raw data (recommended: no)? (y/n) ")
    raw = raw in 'Yy'

    device_config = {}
    device_config['mac_address'] = mac_address
    device_config['acc_frequency'] = int(acc_freq)
    device_config['acc_range'] = int(acc_range)
    device_config['gyro_frequency'] = int(gyro_freq)
    device_config['gyro_range'] = int(gyro_range)
    device_config['raw_data'] = raw
    return device_config


def _get_device_options(device: MetaWearDevice) -> Dict[str, Enum]:
    acc_freq_options = device.return_acc_freq_options()
    gyro_freq_options = device.return_gyro_freq_options()
    acc_range_options = device.return_acc_range_options()
    gyro_range_options = device.return_gyro_range_options()
    if not all([acc_freq_options, gyro_freq_options, acc_range_options, gyro_range_options]):
        print("Device not supported")
        return {}
    device_options = {
        'acc_freq_options': acc_freq_options,
        'gyro_freq_options': gyro_freq_options,
        'acc_range_options': acc_range_options,
        'gyro_range_options': gyro_range_options
    }
    return device_options


def _configure_device(device: MetaWearDevice, device_config: Dict) -> None:
    device_options = _get_device_options(device)
    device.connect()

    device.configure_accelerometer(
        device_options['acc_freq_options'](device_config['acc_frequency']),
        device_options['acc_range_options'](device_config['acc_range'])
    )
    device.configure_gyroscope(
        device_options['gyro_freq_options'](device_config['gyro_frequency']),
        device_options['gyro_range_options'](device_config['gyro_range'])
    )


def enum_to_str(enum: Enum) -> str:
    values = [f"{option.value}: {option.name}" for option in enum]
    return "\n".join(values)

def dict_to_str(dic: Dict) -> str:
    values = [f"{key}: {value}" for key, value in dic.items()]
    return "\n".join(values)

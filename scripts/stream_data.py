"""
Script to stream sensor data from MetaWear device and store in a CSV file.
Provide the MAC address of the MetaWear device as the --address argument.
"""

import argparse

from src.cli import CLI
from src.helpers import stream_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-queue-size", type=int, default=25000, help="Maximum number of data points to store in queue")
    args = parser.parse_args()
    cli = CLI('data')
    user_data = cli.log_in()
    start = input("Do you want to start a session? (y/n) ")
    if start not in 'Yy':
        exit(0)
    print("Starting session...")
    session_id = cli.start_session(user_data['user_id'], user_data['device_id'])
    print(user_data['device_config']['acc_frequency'])
    stream_data(user_data['device'], session_id, args.max_queue_size, args.max_queue_size)


if __name__ == "__main__":
    main()

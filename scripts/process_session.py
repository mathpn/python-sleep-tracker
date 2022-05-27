"""
Temporary script containing the logic to process raw sensor data into a binned
output measuring the amount of movement in each time bin.
"""

import argparse
import numpy as np
import pandas as pd


def cumulative_sum(rows, threshold: int):
    cumsum = 0
    output = []
    for _, row in rows.iterrows():
        timestamp = row.iloc[0]
        acc = np.abs(row.iloc[1])
        cumsum += acc
        if cumsum > threshold:
            output.append(int(timestamp))
            cumsum = 0
    return np.array(output)


def aggregate_data(data, bins: int):
    step_size = data[-1] // bins
    aggregate = []
    for i in range(0, data[-1], step_size):
        aggregate.append(np.count_nonzero((i < data) & (data <= i + step_size)))
    return aggregate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('session_number', type=int)
    parser.add_argument('--bins', type=int, default=20)
    args = parser.parse_args()

    print(f'processing session {args.session_number}')
    acc_data = pd.read_csv(f'data/sessions/{args.session_number}_acc_data.csv')
    gyro_data = pd.read_csv(f'data/sessions/{args.session_number}_gyro_data.csv')

    acc_data_cumulative = cumulative_sum(acc_data, threshold=10)
    gyro_data_cumulative = cumulative_sum(gyro_data, threshold=100)

    timepoints = np.linspace(acc_data.iloc[0, 0], acc_data.iloc[-1, 0], 1000)
    acc_sorted_insert = np.searchsorted(acc_data_cumulative, timepoints)
    acc_aggregate = aggregate_data(acc_sorted_insert, bins=args.bins)
    acc_aggregate = np.array(acc_aggregate)
    print(f'acc_aggregate: \n{np.round(acc_aggregate, 3)}')

    timepoints = np.linspace(gyro_data.iloc[0, 0], gyro_data.iloc[-1, 0], 1000)
    gyro_sorted_insert = np.searchsorted(gyro_data_cumulative, timepoints)
    gyro_aggregate = aggregate_data(gyro_sorted_insert, bins=args.bins)
    gyro_aggregate = np.array(gyro_aggregate)
    print(f'gyro_aggregate: \n{np.round(gyro_aggregate, 3)}')


if __name__ == '__main__':
    main()

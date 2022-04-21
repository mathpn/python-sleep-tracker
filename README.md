# python-sleep-tracker

A sleep tracker written in python that uses sensor data.

## Motivation
I’ve had an old (and as of now unsupported - EOL) [mbientlab](https://github.com/mbientlab) sensor with accelerometer, gyroscope and thermometer for a few years, but I haven’t used it yet (its original use purpose was abandoned).

Therefore, I decided to do this project to track sleep quality using sensor data, just like many smart watches and wear-devices already do using various tools.

**This is still a work in progress**

### Current state
Data is streamed in frequencies of up to 100Hz using BLE 4.0/5.0 .

    - Sensor data can be live-stramed from a MetaWear device with simple preprocessing;
    - Data is stored as a plain csv file (raw or preprocessed).

### Next steps
    - Store data in an organized way with metadata;
    - Postprocessing of time-series data;
    - Descriptive statistics to track sleep;
    - Retrieve and visualize descriptive statistics from stored data.

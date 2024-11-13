# Time series analysis
## Input:
A textual file containing a set of temperature readings
Each line of the file contains one timestamp and the associated temperature reading timestamp, temperature
- The format of the timestamp is the Unix timestamp that is defined as the number of seconds that have elapsed since 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970

The sample rate is 1 minute
- i.e., the difference between the timestamps of two
consecutive readings is 60 seconds

## Output:
Consider all the windows containing 3 consecutive
temperature readings and
- Select the windows characterized by an increasing trend
- A window is characterized by an increasing trend if for all the temperature readings in it **temperature(t)>temperature(t-60 seconds)**
- Store the result into an HDFS file

## Input data
1451606400,12.1
1451606460,12.2
1451606520,13.5
1451606580,14.0
1451606640,14.0
1451606700,15.5
1451606760,15.0

## Output data
1451606400,12.1,1451606460,12.2,1451606520,13.5
1451606460,12.2,1451606520,13.5,1451606580,14.0
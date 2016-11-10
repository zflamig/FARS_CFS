#!/usr/bin/env python
import pandas as pd
import numpy as np
from datetime import timedelta
import subprocess
import sys
import os 

shortNames = ['2-HTGL', '2-HTGL', '0-SFC']
elements = ['DPT', 'TMP', 'APCP']
fieldNames = ['sfcdewpt', 'sfct', 'precip']

# Check if file exists and is not empty
def is_non_zero_file(fpath):
	return True if os.path.isfile(fpath) and os.path.getsize(fpath) > 0 else False

data = pd.read_csv('fars1979.csv', dtype={'YEAR':str, 'MONTH':str, 'DAY':str, 'HOUR':str, 'MINUTE':str})

#filter invalid values
data = data[data.HOUR != '99']
data = data[data.HOUR != '24']

data['date'] = pd.to_datetime(data['YEAR'].str.zfill(4) + '-' + data['MONTH'].str.zfill(2) + '-' + data['DAY'].str.zfill(2) + ' ' + data['HOUR'].str.zfill(2) + ':' + data['MINUTE'].str.zfill(2), format='%Y-%m-%d %H:%M')

# longitude goes from 0-360 in grib files
data['lon'] = data['lon'] + 360.0

data2 = data.head(10)


# Create new columns that we will be saving
for val in fieldNames:
	data[val] = float('nan')

# Iterated through all the FARS data sampling the appropriate model grids
for index, row in data2.iterrows():
	newTime = row['date']
	newTime = newTime.replace(minute=0,second=0,microsecond=0)
	newTime = newTime + timedelta(hours = 3)
	hr = int(np.floor(newTime.hour / 6.0) * 6)
	newTime = newTime.replace(hour=hr)
	fileName = newTime.strftime("pgbh00.gdas.%Y%m%d%H.grb2")
	if not is_non_zero_file(fileName):
		subprocess.call("aws s3 cp s3://noaa-cfs-reanalysis/" + fileName + " . --endpoint-url https://griffin-objstore.opensciencedatacloud.org --profile zac-cdis", shell=True)
		if not is_non_zero_file(fileName):
			continue
	for short, element, field in zip(shortNames, elements, fieldNames):
		command = "/usr/bin/docker run -v /mnt/retrievedata:/mnt gdallocationinfo -valonly -wgs84 -gribelement %s -gribshortname %s /mnt/%s %f %f" % (element, short, fileName, row['lon'], row['lat'])
		s = subprocess.check_output(command.split(' '))
		data.set_value(index, field, float(s))

data.to_csv('out.csv', index=False)

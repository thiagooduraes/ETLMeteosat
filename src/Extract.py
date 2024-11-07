import meteostat
import datetime as dt

import matplotlib.pyplot as plt

# Set time period
end = dt.datetime.now()
start = end - dt.timedelta(days=7)

# Create Point for Montes Claros, MG
location = meteostat.Point(-16.7167, -43.8667, 646)

# Get daily data for 2018
data = meteostat.Daily(location, start, end)
data = data.fetch()

print(data.head())

# Plot line chart including average, minimum and maximum temperature
data.plot(y=['tavg', 'tmin', 'tmax'])
# station
# time
# tavg
# tmin
# tmax
# prcp
# snow
# wdir
# wspd
# wpgt
# pres
# tsun
plt.autoscale()
plt.show()
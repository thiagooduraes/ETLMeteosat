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

# Plot line chart including average, minimum and maximum temperature
data.plot(y=['tavg', 'tmin', 'tmax'])
plt.autoscale()
plt.show()
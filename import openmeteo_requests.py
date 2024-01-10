import openmeteo_requests
from datetime import datetime,timedelta
import requests_cache
import pandas as pd
from retry_requests import retry
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

class mh_weather():
	def __init__(self) -> None:
		pass
	def forecast(self,lat,long):
		self.lat = lat
		self.long = long
		# Make sure all required weather variables are listed here
		# The order of variables in hourly or daily is important to assign them correctly below
		url = "https://customer-api.open-meteo.com/v1/forecast"
		params = {
			"latitude": lat,
			"longitude": long,
			"current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "cloud_cover", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"],
			"hourly": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation_probability", "rain", "cloud_cover", "wind_speed_10m","wind_direction_10m" ,"wind_gusts_10m"]
		}
		responses = openmeteo.weather_api(url, params=params)

		# Process first location. Add a for-loop for multiple locations or weather models
		response = responses[0]
		print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
		print(f"Elevation {response.Elevation()} m asl")
		print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
		print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

		# Current values. The order of variables needs to be the same as requested.
		current = response.Current()
		current_temperature_2m = current.Variables(0).Value()
		current_relative_humidity_2m = current.Variables(1).Value()
		current_apparent_temperature = current.Variables(2).Value()
		current_precipitation = current.Variables(3).Value()
		current_rain = current.Variables(4).Value()
		current_cloud_cover = current.Variables(5).Value()
		current_wind_speed_10m = current.Variables(6).Value()
		current_wind_direction_10m = current.Variables(7).Value()
		current_wind_gusts_10m = current.Variables(8).Value()

		print(f"Current time {current.Time()}")
		print(f"Current temperature_2m {current_temperature_2m}")
		print(f"Current relative_humidity_2m {current_relative_humidity_2m}")
		print(f"Current apparent_temperature {current_apparent_temperature}")
		print(f"Current precipitation {current_precipitation}")
		print(f"Current rain {current_rain}")
		print(f"Current cloud_cover {current_cloud_cover}")
		print(f"Current wind_speed_10m {current_wind_speed_10m}")
		print(f"Current wind_direction_10m {current_wind_direction_10m}")
		print(f"Current wind_gusts_10m {current_wind_gusts_10m}")

		# Process hourly data. The order of variables needs to be the same as requested.
		hourly = response.Hourly()
		hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
		hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
		hourly_apparent_temperature = hourly.Variables(2).ValuesAsNumpy()
		hourly_precipitation_probability = hourly.Variables(3).ValuesAsNumpy()
		hourly_rain = hourly.Variables(4).ValuesAsNumpy()
		hourly_cloud_cover = hourly.Variables(5).ValuesAsNumpy()
		hourly_wind_speed_10m = hourly.Variables(6).ValuesAsNumpy()
		hourly_wind_gusts_10m = hourly.Variables(7).ValuesAsNumpy()

		hourly_data = {"date": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s"),
			end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		)}
		hourly_data["temperature_2m"] = hourly_temperature_2m
		hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
		hourly_data["apparent_temperature"] = hourly_apparent_temperature
		hourly_data["precipitation_probability"] = hourly_precipitation_probability
		hourly_data["rain"] = hourly_rain
		hourly_data["cloud_cover"] = hourly_cloud_cover
		hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
		hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

		hourly_dataframe = pd.DataFrame(data = hourly_data)
		return hourly_dataframe
	

	def historical(self,lat,long,start_date, end_date):
		self.lat = lat
		self.long = long
		self.start_date = start_date
		self.end_date = end_date
		# Setup the Open-Meteo API client with cache and retry on error
		cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
		retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
		openmeteo = openmeteo_requests.Client(session = retry_session)

		# Make sure all required weather variables are listed here
		# The order of variables in hourly or daily is important to assign them correctly below
		url = "https://archive-api.open-meteo.com/v1/archive"
		params = {
			"latitude": self.lat, #52.52,
			"longitude":self.long, #13.41,
			"start_date": self.start_date, #"2023-12-24",
			"end_date": self.end_date, #"2024-01-07",
			"hourly": ["temperature_2m", "apparent_temperature", "rain", "cloud_cover", "wind_speed_10m", "wind_gusts_10m"],
			"daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]
		}
		responses = openmeteo.weather_api(url, params=params)

		# Process first location. Add a for-loop for multiple locations or weather models
		response = responses[0]
		print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
		print(f"Elevation {response.Elevation()} m asl")
		print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
		print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

		# Process hourly data. The order of variables needs to be the same as requested.
		hourly = response.Hourly()
		hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
		hourly_apparent_temperature = hourly.Variables(1).ValuesAsNumpy()
		hourly_rain = hourly.Variables(2).ValuesAsNumpy()
		hourly_cloud_cover = hourly.Variables(3).ValuesAsNumpy()
		hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()
		hourly_wind_gusts_10m = hourly.Variables(5).ValuesAsNumpy()

		hourly_data = {"date": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s"),
			end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		)}
		hourly_data["temperature_2m"] = hourly_temperature_2m
		hourly_data["apparent_temperature"] = hourly_apparent_temperature
		hourly_data["rain"] = hourly_rain
		hourly_data["cloud_cover"] = hourly_cloud_cover
		hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
		hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

		hourly_dataframe_historical = pd.DataFrame(data = hourly_data)
		return hourly_dataframe_historical
	

df_geo = pd.read_csv(r"D:\Projects\Maharashtra_Project\weather_data_open_meteo\geo_code_MH.csv")
df_geo[['lat','long']]=df_geo['Geocode'].str.split(pat=',',expand=True)

start_date = (datetime.now() - timedelta(0)).strftime('%Y-%m-%d')
end_date = (datetime.now() - timedelta(7)).strftime('%Y-%m-%d')
print(start_date,end_date)
df_forecast = pd.DataFrame()
df_historical = pd.DataFrame()

klass = mh_weather()
for index,row in df_geo.iterrows:
	try:
		lat = row['lat']
		long = row['long']
		df_temp_fore = klass.forecast(lat,long)
		df_temp_fore['lat'] = row['lat']
		df_temp_fore['long'] = row['long']
		df_forecast = pd.concat([df_temp_fore,df_forecast])

		df_temp_hist = klass.historical(lat,long,start_date=start_date,end_date=end_date)
		df_temp_fore['lat'] = row['lat']
		df_temp_fore['long'] = row['long']
		df_historical = pd.concat([df_temp_hist,df_historical])
	except:
		print("No data is available for geocode ", row['lat'], row['long'])

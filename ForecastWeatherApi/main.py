import time
import requests
import logging
import pandas as pd

logging.basicConfig(filename='errors.log', level=logging.WARNING, encoding='utf-8',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class CapitalCity:
    def __init__(self, countryName, capitalName, latitude, longitude):
        self._countryName = countryName
        self._capitalName = capitalName
        self._latitude = latitude
        self._longitude = longitude

    @property
    def countryName(self):
        return self._countryName

    @property
    def capital_name(self):
        return self._capitalName

    @property
    def latitude(self):
        return self._latitude

    @property
    def longitude(self):
        return self._longitude

class noDataFromApiException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class missingRequiredDataException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)



def getCountries():
    try:
        countries_url = 'https://restcountries.com/v3.1/all'
        response = requests.get(countries_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(err)
        logging.warning(f'RequestCity request failed: {err}')


def getCityWeatherHistory(city:CapitalCity):
    try:
        weather_url = f'https://api.open-meteo.com/v1/forecast?latitude={city.latitude}&longitude={city.longitude}&forecast_days=1&past_days=1&daily=weather_code,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,sunrise,sunset,daylight_duration,sunshine_duration,uv_index_max,uv_index_clear_sky_max,rain_sum,showers_sum,snowfall_sum,precipitation_sum,precipitation_hours,precipitation_probability_max,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration'
        response = requests.get(weather_url)
        response.raise_for_status()
        json_data = response.json()
        json_data["country_name"] = city.countryName
        json_data["capital_name"] = city.capital_name

        daily_index = 0 # Even while using timezones with API, at certain hours the user receives dataset for the current and previous day.
        if len(json_data["daily"]["weather_code"])>1:
            daily_index = 1

        json_data["weather_code"]=json_data["daily"]["weather_code"][daily_index]
        json_data["temperature_2m_max"]=json_data["daily"]["temperature_2m_max"][daily_index]
        json_data["temperature_2m_min"]=json_data["daily"]["temperature_2m_min"][daily_index]
        json_data["apparent_temperature_max"]=json_data["daily"]["apparent_temperature_max"][daily_index]
        json_data["apparent_temperature_min"]=json_data["daily"]["apparent_temperature_min"][daily_index]
        json_data["sunrise"]=json_data["daily"]["sunrise"][daily_index]
        json_data["sunset"]=json_data["daily"]["sunset"][daily_index]
        json_data["daylight_duration"]=json_data["daily"]["daylight_duration"][daily_index]
        json_data["sunshine_duration"]=json_data["daily"]["sunshine_duration"][daily_index]
        json_data["uv_index_max"]=json_data["daily"]["uv_index_max"][daily_index]
        json_data["uv_index_clear_sky_max"]=json_data["daily"]["uv_index_clear_sky_max"][daily_index]
        json_data["rain_sum"]=json_data["daily"]["rain_sum"][daily_index]
        json_data["showers_sum"]=json_data["daily"]["showers_sum"][daily_index]
        json_data["snowfall_sum"]=json_data["daily"]["snowfall_sum"][daily_index]
        json_data["precipitation_sum"]=json_data["daily"]["precipitation_sum"][daily_index]
        json_data["precipitation_hours"]=json_data["daily"]["precipitation_hours"][daily_index]
        json_data["precipitation_probability_max"]=json_data["daily"]["precipitation_probability_max"][daily_index]
        json_data["wind_speed_10m_max"]=json_data["daily"]["wind_speed_10m_max"][daily_index]
        json_data["wind_gusts_10m_max"]=json_data["daily"]["wind_gusts_10m_max"][daily_index]
        json_data["wind_direction_10m_dominant"]=json_data["daily"]["wind_direction_10m_dominant"][daily_index]
        json_data["shortwave_radiation_sum"]=json_data["daily"]["shortwave_radiation_sum"][daily_index]
        json_data["et0_fao_evapotranspiration"]=json_data["daily"]["et0_fao_evapotranspiration"][daily_index]

        return json_data
    except requests.exceptions.RequestException as err:
        print(err)
        logging.warning(f'Request Exception Weather request failed: {err}')


def removeWeatherKeysFromJsonFile(weatherlist):
    temp = weatherlist.copy()
    weatherlist.clear()

    if not temp:
        raise missingRequiredDataException('No data found in provided weather List')

    try:
        for jsonfile in temp:
            prepared_keys_to_delete = []  # to avoid the error of deleting dictionary rows while iterating through it
            if "et0_fao_evapotranspiration" in jsonfile:
                prepared_keys_to_delete.append("et0_fao_evapotranspiration")
            if "timezone_abbreviation" in jsonfile:
                prepared_keys_to_delete.append("timezone_abbreviation")
            if "elevation" in jsonfile:
                prepared_keys_to_delete.append("elevation")
            if "daily_units" in jsonfile:
                prepared_keys_to_delete.append("daily_units")
            if "daily" in jsonfile:
                prepared_keys_to_delete.append("daily")
            for prepared_key in prepared_keys_to_delete:
                del jsonfile[prepared_key]

            weatherlist.append(jsonfile)

        return weatherlist
    except KeyError as err:
        print(err)
        logging.warning(f'KeyError Tried to remove the column that is not in the dataset: {err}')




capitalList = []
try:
    CountriesData = getCountries()
    if not CountriesData:
        raise noDataFromApiException("No data pulled from the Countries API")

    for country in CountriesData:
        try:
            if ('name' in country and country['name'] and 'common' in country['name'] and country['name']['common']
                    and 'capital' in country and country['capital'] and 'capitalInfo' in country
                    and 'latlng' in country['capitalInfo'] and len(country['capitalInfo']['latlng']) == 2):

                capitalList.append(
                    CapitalCity(country['name']['common'], country['capital'][0], country['capitalInfo']['latlng'][0],
                                country['capitalInfo']['latlng'][1]))
            else:
                if 'name' in country and country['name']:
                    if country["name"] == "Antarctic" or country["name"] == "Nauru" or country["name"] == "Antarctica"\
                            or country["name"] == "Macau" or country["name"] == "United States Minor OutlyingIslands"\
                            or country["name"] == "Heard Island and McDonald Islands" or country["name"] == "BouvetIsland":
                        pass
                    else:
                        logging.warning(f"Could not extract capital information for {country['name']['common']}")

        except KeyError as err:
            logging.warning(f'Key Error Missing key in capital data {err}')
            print(err)

except noDataFromApiException as error:
    logging.warning(f'NoDataFromApiException Missing Country Api Data{error}')


weatherJsonList = []
try:
    if not capitalList:
        raise noDataFromApiException("Missing Data from capitalList that was pulled from the Countries API")

    for capital in capitalList:
        time.sleep(0.15) # to avoid too many requests error
        weatherJsonList.append(getCityWeatherHistory(capital))
        print(f'added weather info for {capital.capital_name}')

    weatherJsonList = removeWeatherKeysFromJsonFile(weatherJsonList)

except missingRequiredDataException as error:
    print(error)
    logging.warning(f'Missing Required Data Exception: {error}')
except noDataFromApiException as error:
    print(error)
    logging.warning(f'noDataFromApiException Missing Data{error}')


# Saving to CSV
try:
    df = pd.DataFrame(weatherJsonList)
    df.to_csv('weather.csv', index=False, encoding='utf-8')

except ValueError as valueError: #for example only one element in list without providing index while creating dataframe in pandas
    print(valueError)
    logging.warning(f'ValueError Creating DataframeError{valueError}')
except IOError as ioError: #saving to csv file.
    print(ioError)
    logging.warning(f'IOError {ioError}')
except UnicodeEncodeError as unicodeError: #if the data contains characters than cannot be encoded with default encoder
    print(unicodeError)
    logging.warning(f'UnicodeEncodeError {unicodeError}')






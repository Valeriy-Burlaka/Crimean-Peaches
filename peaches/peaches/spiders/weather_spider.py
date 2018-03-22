import calendar
from statistics import mean

import scrapy


def month_dates(year, month):
    """
    Return a list of `datetime.date`s for a given month.

    :type year: int
    :type month: int
    :return type: list
    """
    c = calendar.Calendar()
    # this iterator always yields complete weeks, so we need to filter out
    # dates that are outside of the specified month.
    dates = c.itermonthdates(year, month)
    dates = [date for date in dates if date.month == month]

    return dates


class WeatherSpider(scrapy.Spider):
    name = "weather"

    def start_requests(self):
        url_template = "http://allcrimea.net/weather/arhiv/{year}-{month}-{day}/"
        for year in [2013, 2014, 2015, 2016]:
            for month in range(1, 13):
                dates = month_dates(year, month)
                for date in dates:
                    url = url_template.format(year=year, month=month, day=date.day)
                    yield scrapy.Request(url=url, callback=self.parse)

    def _weather_data(self, city_name, date, data):
        template = "{name},{date},{time},{temp_min},{temp_max},{atm},{press},{hum},{wind_dir},{wind_str}\n"

        _time = data.css("td::text")[0].extract().split(",")[0]
        temperature = data.css("td")[1].css("b::text").extract_first()
        temp_min = int(temperature.split("/")[0])
        temp_max = int(temperature.split("/")[1])
        atmosphere = "; ".join(data.css("td")[2].css("*::text").extract())
        pressure = data.css("td::text")[3].extract().strip(" мм")
        pressure = mean(int(v) for v in pressure.split("-"))
        humidity = data.css("td::text")[4].extract().strip("%")
        humidity = mean(int(v) for v in humidity.split("-"))
        wind = data.css("td")[5].css("*::text").extract()
        wind_direction = wind[0]
        wind_strength = wind[1].lstrip("-").rstrip(" м/с")
        wind_strength = mean(int(v) for v in wind_strength.split("-"))

        return template.format(name=city_name,
                               date=date,
                               time=_time,
                               temp_min=temp_min,
                               temp_max=temp_max,
                               atm=atmosphere,
                               press=pressure,
                               hum=humidity,
                               wind_dir=wind_direction,
                               wind_str=wind_strength)

    def parse(self, response):
        date = response.url.split("/")[-2]
        date = "-".join(d.zfill(2) for d in date.split("-"))
        year = date.split("-")[0]

        all_cities = response.css(".weather_table > table")
        all_extracted = []
        for city in all_cities:
            city_data = city.css("tr[valign=top]")
            city_parsed = []
            city_name = city_data[0].css("b::text").extract_first()
            for data in city_data[1:]:
                city_parsed.append(self._weather_data(city_name, date, data))
            all_extracted.extend(city_parsed)

        with open("./data/Crimean_weather-{}.csv".format(year), "a") as f:
            for row in all_extracted:
                f.write(row)

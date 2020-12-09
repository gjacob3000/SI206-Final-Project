import requests
from bs4 import BeautifulSoup
import json
import sqlite3 
import os
import time
import math

AIRVISUALKEY = "f6a636a5-5d43-4c10-ac4b-15b0b84fab61"

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    curr = conn.cursor()
    return curr, conn

def createTables(curr,conn):

    curr.execute("CREATE TABLE IF NOT EXISTS CountryCases (name TEXT PRIMARY KEY, cases INTEGER, deaths INTEGER, population INTEGER, LE INTEGER, lat NUMBER, lon NUMBER)")
    curr.execute("CREATE TABLE IF NOT EXISTS CountryAQIs (name TEXT PRIMARY KEY, aqi INTEGER, color TEXT)")
    
    curr.execute("CREATE TABLE IF NOT EXISTS AQIColors (color TEXT PRIMARY KEY, level_of_concern TEXT)")
    curr.execute("INSERT OR IGNORE INTO AQIColors (color, level_of_concern) VALUES (?,?)", ("Green", "Good"))
    curr.execute("INSERT OR IGNORE INTO AQIColors (color, level_of_concern) VALUES (?,?)", ("Yellow", "Moderate"))
    curr.execute("INSERT OR IGNORE INTO AQIColors (color, level_of_concern) VALUES (?,?)", ("Orange", "Unhealthy for Sensitive Groups"))
    curr.execute("INSERT OR IGNORE INTO AQIColors (color, level_of_concern) VALUES (?,?)", ("Red", "Unhealthy"))
    curr.execute("INSERT OR IGNORE INTO AQIColors (color, level_of_concern) VALUES (?,?)", ("Purple", "Very Unhealthy"))
    curr.execute("INSERT OR IGNORE INTO AQIColors (color, level_of_concern) VALUES (?,?)", ("Maroon", "Hazardous"))
    conn.commit()

def getPollutionData(curr, conn, ):
    countries = []
    try:
        url = "https://www.numbeo.com/pollution/rankings_by_country.jsp"
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        values = soup.find_all("tr", {'style': 'width: 100%'})
    
        aqi_data = []
        

        for val in values:
            name = val.find('td', class_ = 'cityOrCountryInIndicesTable').text 
            if name == "United States":
                name = "US"
            aqis = val.find_all('td', {'style':'text-align: right'})
            aqi = float(aqis[1].text)
            level = ""
            if aqi <= 50:
                level = "Good"
            elif aqi <= 100:
                level = "Moderate"
            elif aqi <= 150:
                level = "Unhealthy for Sensitive Groups"
            elif aqi <= 200:
                level = "Unhealthy"
            elif aqi <= 300:
                level = "Very Unhealthy"
            elif aqi > 301:
                level = "Hazardous"
            
            curr.execute("SELECT color FROM AQIColors WHERE level_of_concern = ?", (level,))
            color = curr.fetchone()[0]

            aqi_data.append((name, aqi, color),)
        
        curr.execute("SELECT name FROM CountryAQIs")
        table_length = len(curr.fetchall())

        
        for i in range(25):
            if (table_length + i) < len(aqi_data): #need to make sure value is within index of aqi_data
                curr.execute("INSERT OR IGNORE INTO CountryAQIs (name, aqi, color) VALUES (?,?, ?)", (aqi_data[table_length + i][0], aqi_data[table_length + i][1], aqi_data[table_length + i][2]))
                countries.append(aqi_data[table_length + i][0])
                
    except:
        print("error when reading from url")
    
    conn.commit()
    return countries


def getCovidApiData(curr, conn, countries):
    try:
        url = "https://covid-api.mmediagroup.fr/v1/cases"
        r = requests.get(url)
        dict_list = json.loads(r.text)

        name = ""
        confirmed = -1
        deaths = -1
        population = 0
        life_expectancy = 0.0
        lat = 0.0
        lon = 0.0
        count = 0

        for country in countries:
            if count < 25:
                if country in dict_list:
                    item = dict_list[country]
                    name = country
                    confirmed = item["All"]["confirmed"]
                    deaths = item["All"]["deaths"]

                    if "population" in item["All"]:
                        population = item["All"]["population"] 
                    if "life_expectancy" in item["All"]:
                        life_expectancy = item["All"]["life_expectancy"]
                    if "lat" in item["All"]:
                        lat = item["All"]["lat"]
                    if "long" in item["All"]:
                        lon = item["All"]["long"]

                    curr.execute("INSERT OR IGNORE INTO CountryCases (name, cases, deaths, population, LE, lat, lon) VALUES (?,?,?,?,?,?,?)", (name,confirmed,deaths,population,life_expectancy,lat,lon))
                    count += 1
                else:
                    removeFromData(curr, conn, country)

    except:
        print("error when accessing Covid API")
        dict_list = []

    conn.commit()

#if any air quality points are too inaccurate we will remove the country from our analysis
def removeFromData(curr, conn, name):
    curr.execute("DELETE FROM CountryAQIs WHERE name = ?", (name,))
    conn.commit()


def main():
    curr,conn = setUpDatabase("covid_data.db")
    #dropTablesForDebugging(curr,conn)
    createTables(curr,conn)
    #loading API
    countries = getPollutionData(curr,conn)
    getCovidApiData(curr,conn, countries)
    
    
    
    print("Successfully ran main!")

if __name__ == "__main__":
    main()
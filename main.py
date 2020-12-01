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
    #curr.execute("DROP TABLE IF EXISTS Countries")
    curr.execute("CREATE TABLE IF NOT EXISTS Countries (id INTEGER PRIMARY KEY, name TEXT)")
    #curr.execute("DROP TABLE IF EXISTS CountryCases")
    curr.execute("CREATE TABLE IF NOT EXISTS CountryCases (name TEXT PRIMARY KEY, cases INTEGER, deaths INTEGER, population INTEGER, LE INTEGER, lat NUMBER, lon NUMBER)")
    #curr.execute("DROP TABLE IF EXISTS CountryAQIs")
    curr.execute("CREATE TABLE IF NOT EXISTS CountryAQIs (name TEXT PRIMARY KEY, city TEXT, aqi INTEGER)")

    curr.execute("CREATE TABLE IF NOT EXISTS AirWebAQIs (country TEXT PRIMARY KEY, aqi2019 NUMBER, aqi2018 NUMBER)")

def getPollutionData(curr,conn):
    try:
        curr.execute("SELECT country,aqi2019,aqi2018 FROM AirWebAQIs")
        country_tuple = curr.fetchall()
        countries = []
        for country in country_tuple:
            countries.append(country[0])
        url = "https://www.iqair.com/us/world-most-polluted-countries"
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        val = soup.find_all('tr')
        count = 0 
        for values in val:
            if count < 25:
                names = values.find_all('div', class_ = "country-name")
                averages = values.find_all('span')
                if len(names) == 0:
                    continue
                country = ""
                for name in names:
                    country =  name.getText()
                aqi2019 = 0
                aqi2018 = 0

                if(averages[0].text != " - "):
                    aqi2019 = float(averages[0].text)

                if(averages[1].text != " - "):
                    aqi2018 = float(averages[1].text)
                    
                if country not in countries:
                    curr.execute("INSERT OR IGNORE INTO AirWebAQIs (country, aqi2019, aqi2018) VALUES (?,?,?)", (country, aqi2019, aqi2018))
                    count+=1
        #print(val)

    except:
        print("error when reading from url")
        dict_list = []
    conn.commit()
    return

def getPollutionApiData(curr,conn):
    try:
        curr.execute("SELECT name,lat,lon FROM CountryCases")
        list_of_coords = curr.fetchall()
        count = 0 #this checks how many countries we actually add to the database that aren't repeats
        run = 0 #using this to keep track of how many times we call on the api
        for coord in list_of_coords:
            if count < 25: #using to make sure we only bring in 25 data points per run
                url = "http://api.airvisual.com/v2/nearest_city?lat="+str(coord[1])+"&lon="+str(coord[2])+"&key="+AIRVISUALKEY
                r = requests.get(url)
                dict_list = json.loads(r.text)
                if dict_list["data"]["status"] == "fail":
                    print("fail")
                    conn.commit()
                    return
                covid_lat = coord[1]
                covid_lon = coord[2]
                aqi_lat = dict_list["data"]["location"]["coordinates"][1]
                aqi_lon = dict_list["data"]["location"]["coordinates"][0]
                aqi_close = isAQIClose(covid_lat, covid_lon, aqi_lat, aqi_lon)
                
                if aqi_close is False or (dict_list["data"] == "no_nearest_station"):
                    print(coord[0])
                    print(dict_list["data"]["country"])
                    print("wrong country!")
                    #removeFromData(curr, conn, coord[0])
                else:
                    name = dict_list["data"]["country"]
                    city = dict_list["data"]["city"]
                    aqi = dict_list["data"]["current"]["pollution"]["aqius"]

                    curr.execute("SELECT name FROM CountryAQIs WHERE name = ?", (name,))
                    check = curr.fetchone()
    
                    if check is None:
                        curr.execute("INSERT OR IGNORE INTO CountryAQIs (name, city, aqi) VALUES (?,?,?)", (name, city, aqi))
                        count += 1
                
            run += 1
            #cant pull more than 10 per minute so we need to pause the system for a few seconds before collecting more
            if run % 9 == 0 and run != 0: 
                print("Pausing for a bit...")
                time.sleep(60) # this pauses the program to give api a break
        conn.commit()    
        
    except:
        print("error when reading from url")
        dict_list = []
        conn.commit()  
    conn.commit()  
    return

def getCovidApiData(curr, conn):
    try:
        url = "https://covid-api.mmediagroup.fr/v1/cases"
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
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
        for items in dict_list.items():
            if count < 25: #using to make sure we only bring in 25 data points per run
                name = items[0]
                confirmed = items[1]["All"]["confirmed"]
                deaths = items[1]["All"]["deaths"]

                if "population" in items[1]["All"]:
                    population = items[1]["All"]["population"] 
                if "life_expectancy" in items[1]["All"]:
                    life_expectancy = items[1]["All"]["life_expectancy"]
                if "lat" in items[1]["All"]:
                    lat = items[1]["All"]["lat"]
                if "long" in items[1]["All"]:
                    lon = items[1]["All"]["long"] 

                #using this to check if value already exists in table
                curr.execute("SELECT name FROM Countries WHERE name = ?", (name,))
                check = curr.fetchone()
    
                if check is None:
                    curr.execute("INSERT OR IGNORE INTO CountryCases (name, cases, deaths, population, LE, lat, lon) VALUES (?,?,?,?,?,?,?)", (name,confirmed,deaths,population,life_expectancy,lat,lon))
                    curr.execute("INSERT OR IGNORE INTO Countries (name) VALUES (?)", (name,))
                    count += 1

            #print((name,confirmed, deaths, population, life_expectancy, lat, lon))
        conn.commit()

    except:
        print("error when accessing Covid API")
        dict_list = []

#if any air quality points are too inaccurate we will remove the country from our analysis
def removeFromData(curr, conn, name):
    curr.execute("DELETE FROM Countries WHERE name = ?", (name,))
    curr.execute("DELETE FROM CountryCases WHERE name = ?", (name,))
    conn.commit()

#checking to see if air quality loc is close to covid loc
#making the dist <= 6 means the air quality location is within 300 miles
def isAQIClose(covid_lat, covid_lon, aqi_lat, aqi_lon):
    lat_diff = (covid_lat - aqi_lat)**2
    lon_diff = (covid_lon - aqi_lon)**2
    dist = math.sqrt(lat_diff + lon_diff)

    if dist <= 6:
        return True
    else:
        return False




def main():
    curr,conn = setUpDatabase("covid_data.db")
    createTables(curr,conn)
    #loading API
    getCovidApiData(curr,conn)
    #getPollutionApiData(curr,conn)
    getPollutionData(curr,conn)
    url = "https://www.numbeo.com/pollution/rankings_by_country.jsp"
    r = requests.get(url)
    print(r.text)
    print("hello world!")

if __name__ == "__main__":
    main()
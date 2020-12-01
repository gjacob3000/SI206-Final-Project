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
    curr.execute("CREATE TABLE IF NOT EXISTS CountryAQIs (name TEXT PRIMARY KEY, aqi INTEGER)")

def getPollutionData(curr, conn, ):
    countries = []
    try:
        url = "https://www.numbeo.com/pollution/rankings_by_country.jsp"
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        values = soup.find_all("tr", {'style': 'width: 100%'})
    
        aqi_data = []
        

        for val in values:
            name = val.find('td', class_ = 'cityOrCountryInIndicesTable').text 
            aqi = val.find('td', {'style':'text-align: right'}).text 
            aqi_data.append((name, aqi),)
        
        curr.execute("SELECT name FROM CountryAQIs")
        table_length = len(curr.fetchall())

        
        for i in range(25):
            if (table_length + i) < len(aqi_data): #need to make sure value is within index of aqi_data
                curr.execute("INSERT OR IGNORE INTO CountryAQIs (name, aqi) VALUES (?,?)", (aqi_data[table_length + i][0], aqi_data[table_length + i][1]))
                countries.append(aqi_data[table_length + i][0])
                
    except:
        print("error when reading from url")
    
    conn.commit()
    return countries


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
        """
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
        """

    except:
        print("error when accessing Covid API")
        dict_list = []

    conn.commit()

#if any air quality points are too inaccurate we will remove the country from our analysis
def removeFromData(curr, conn, name):
    curr.execute("DELETE FROM CountryAQIs WHERE name = ?", (name,))
    conn.commit()

def dropTablesForDebugging(curr,conn):
    curr.execute("DROP TABLE IF EXISTS CountryCases")
    curr.execute("DROP TABLE IF EXISTS CountryAQIs")

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
    #dropTablesForDebugging(curr,conn)
    createTables(curr,conn)
    #loading API
    countries = getPollutionData(curr,conn)
    getCovidApiData(curr,conn, countries)
    
    
    
    print("hello world!")

if __name__ == "__main__":
    main()
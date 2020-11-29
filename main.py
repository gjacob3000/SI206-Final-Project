import requests
from bs4 import BeautifulSoup
import json
import sqlite3 
import os

AIRVISUALKEY = "f6a636a5-5d43-4c10-ac4b-15b0b84fab61"

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    curr = conn.cursor()
    return curr, conn

def createTables(curr,conn):
    curr.execute("DROP TABLE IF EXISTS Countries")
    curr.execute("CREATE TABLE Countries (id INTEGER PRIMARY KEY, name TEXT)")
    curr.execute("DROP TABLE IF EXISTS CountryCases")
    curr.execute("CREATE TABLE CountryCases (name TEXT PRIMARY KEY, cases INTEGER, deaths INTEGER, population INTEGER, LE INTEGER, lat NUMBER, lon NUMBER)")

def getPollutionData():
    try:
        url = "https://www.iqair.com/us/world-most-polluted-countries"
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        val = soup.find_all('td')
        #print(val)
    except:
        print("error when readong from url")
        dict_list = []
    return

def getPollutionApiData(curr,conn):
    try:
        curr.execute("SELECT name,lat,lon FROM CountryCases")
        list_of_coords = curr.fetchall()
        for coord in list_of_coords:
            url = "http://api.airvisual.com/v2/nearest_city?lat="+str(coord[1])+"&lon="+str(coord[2])+"&key="+AIRVISUALKEY
            r = requests.get(url)
            dict_list = json.loads(r.text)
            if(coord[0] != dict_list["data"]["country"]):
                print(coord[0])
                print(dict_list["data"]["country"])
                print("wrong country!")
            else:
                print(dict_list)
    except:
        print("error when readong from url")
        dict_list = []
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
        for items in dict_list.items():

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

            curr.execute("INSERT OR IGNORE INTO CountryCases (name, cases, deaths, population, LE, lat, lon) VALUES (?,?,?,?,?,?,?)", (name,confirmed,deaths,population,life_expectancy,lat,lon))
            curr.execute("INSERT OR IGNORE INTO Countries (name) VALUES (?)", (name,))

            print((name,confirmed, deaths, population, life_expectancy, lat, lon))
        conn.commit()

    except:
        print("error when accessing API")
        dict_list = []


def main():
    curr,conn = setUpDatabase("covid_data.db")
    createTables(curr,conn)
    #loading API
    getCovidApiData(curr,conn)
    getPollutionApiData(curr,conn)

    print("hello world!")

if __name__ == "__main__":
    main()
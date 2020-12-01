import json
import sqlite3 
import os
import time
import math
import plotly.graph_objects as p


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    curr = conn.cursor()
    return curr, conn
def writeToFile(global_cases, global_deaths, global_population, total_aqi, country_dict, num_data):
    # need to write to file
    f = open("processed_data.txt", "w")
    f.writelines("----------------- GLOBAL DATA -----------------\n")
    f.write("Total Cases: " + str(global_cases) + "\n")
    f.write("Total Deaths: " + str(global_deaths) + "\n")
    f.write("Total Population: " + str(global_population) + "\n")
    f.write("Mortality Rate: " + str(global_deaths/global_cases) + "\n")
    f.write("% Population Infected: "+ str(100* global_cases/global_population) + "\n")
    f.write("------------------ AVERAGES -------------------\n")
    f.write("Average Cases Per Country: " + str(global_cases/num_data) + "\n")
    f.write("Average Deaths Per Country: " + str(global_deaths/num_data) + "\n")
    f.write("Average AQI Per Country: " + str(total_aqi/num_data) + "\n")
    f.write("---------------- COUNTRY DATA -----------------\n")
    for item in country_dict.items():
        f.write(item[0]+":\n")
        f.write("     Cases by AQI: " + str(item[1][0]) + "\n")
        f.write("     Cases by Population: " + str(item[1][1]) + "\n")
        f.write("     Mortality Rate: " + str(item[1][2]) + "\n")

    f.close()
    

def createPlots():
    #hi
    print("hello")

def main():
    #get all data
    curr,conn = setUpDatabase("covid_data.db")
    curr.execute("SELECT * FROM CountryCases JOIN CountryAQIs ON CountryCases.name = CountryAQIs.name")
    list_all = curr.fetchall()
    # name cases deaths population LE lat lon | name aqi
    #  0     1     2         3      4  5   6      7   8
    total_cases = 0
    num_elements = len(list_all)
    country_dict = {}
    total_population = 0
    total_deaths = 0
    total_aqi = 0
    list_all = sorted(list_all)
    for country_data in list_all:

        total_cases += country_data[1]
        total_population += country_data[3]
        total_deaths += country_data[2]
        total_aqi += country_data[8]

        cases_by_aqi = country_data[1]/country_data[8]
        cases_by_pop = country_data[1]/country_data[3]
        deaths_by_cases = country_data[2]/country_data[1]
        country_dict[country_data[0]] = (cases_by_aqi,cases_by_pop, deaths_by_cases)
    
    
    writeToFile(total_cases,total_deaths,total_population,total_aqi,country_dict,num_elements)   
    
    #get average number of deaths per country

if __name__ == "__main__":
    main()
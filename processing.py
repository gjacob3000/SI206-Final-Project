import json
import sqlite3 
import os
import time
import math
import plotly.graph_objects as go


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    curr = conn.cursor()
    return curr, conn
def writeToFile(global_cases, global_deaths, global_population, total_aqi, color_avg_cases, color_avg_deaths, country_dict, num_data):
    # need to write to file
    f = open("processed_data.txt", "w")
    f.writelines("----------------- ALL COUNTRY DATA -----------------\n")
    f.write("Total Cases: " + str(global_cases) + "\n")
    f.write("Total Deaths: " + str(global_deaths) + "\n")
    f.write("Total Population: " + str(global_population) + "\n")
    f.write("Mortality Rate: " + str(global_deaths/global_cases) + "\n")
    f.write("% Population Infected: "+ str(100* global_cases/global_population) + "\n")
    f.write("------------------ AVERAGES -------------------\n")
    f.write("Average Cases Per Country: " + str(global_cases/num_data) + "\n")
    f.write("Average Deaths Per Country: " + str(global_deaths/num_data) + "\n")
    f.write("Average AQI Per Country: " + str(total_aqi/num_data) + "\n")
    f.write("------------------ AVERAGES BASED ON AQI COLOR -------------------\n")
    f.write("GREEN:" + "\n")
    f.write("     Average Cases: " + str(color_avg_cases[0]) + "\n")
    f.write("     Average Deaths: " + str(color_avg_deaths[0]) + "\n")
    f.write("YELLOW:" + "\n")
    f.write("     Average Cases: " + str(color_avg_cases[1]) + "\n")
    f.write("     Average Deaths: " + str(color_avg_deaths[1]) + "\n")
    f.write("ORANGE:" + "\n")
    f.write("     Average Cases: " + str(color_avg_cases[2]) + "\n")
    f.write("     Average Deaths: " + str(color_avg_deaths[2]) + "\n")
    f.write("RED:" + "\n")
    f.write("     Average Cases: " + str(color_avg_cases[3]) + "\n")
    f.write("     Average Deaths: " + str(color_avg_deaths[3]) + "\n")
    f.write("---------------- COUNTRY DATA -----------------\n")
    for item in country_dict.items():
        f.write(item[0]+":\n")
        f.write("     Cases by AQI: " + str(item[1][0]) + "\n")
        f.write("     AQI Color Status: " + str(item[1][3]) + "\n")
        f.write("     Cases by Population: " + str(item[1][1]) + "\n")
        f.write("     Mortality Rate: " + str(item[1][2]) + "\n")

    f.close()
    

#thinking we could make an histogram with this data
def aqiColorAverages(country_dict):
    color_avg_cases = []
    green_cases = []
    yellow_cases = []
    orange_cases = []
    red_cases = []

    color_avg_deaths = []
    green_deaths = []
    yellow_deaths = []
    orange_deaths = []
    red_deaths = []
    
    for item in country_dict.items():
        if item[1][3] == 'Green':
            green_cases.append(item[1][0])
            green_deaths.append(item[1][1])
        elif item[1][3] == "Yellow":
            yellow_cases.append(item[1][0])
            yellow_deaths.append(item[1][1])
        elif item[1][3] == "Orange":
            orange_cases.append(item[1][0])
            orange_deaths.append(item[1][1])
        elif item[1][3] == "Red":
            red_cases.append(item[1][0])
            red_deaths.append(item[1][1])

    green_avg_cases = sum(green_cases)/len(green_cases)
    color_avg_cases.append(green_avg_cases)
    yellow_avg_cases = sum(yellow_cases)/len(yellow_cases)
    color_avg_cases.append(yellow_avg_cases)
    orange_avg_cases = sum(orange_cases)/len(orange_cases)
    color_avg_cases.append(orange_avg_cases)
    red_avg_cases = sum(red_cases)/len(red_cases)
    color_avg_cases.append(red_avg_cases)

    green_avg_deaths = sum(green_deaths)/len(green_deaths)
    color_avg_deaths.append(green_avg_deaths)
    yellow_avg_deaths = sum(yellow_deaths)/len(yellow_deaths)
    color_avg_deaths.append(yellow_avg_deaths)
    orange_avg_deaths = sum(orange_deaths)/len(orange_deaths)
    color_avg_deaths.append(orange_avg_deaths)
    red_avg_deaths = sum(red_deaths)/len(red_deaths)
    color_avg_deaths.append(red_avg_deaths)

    return color_avg_cases, color_avg_deaths

def createGraphs(country_names, country_cases_by_pop, country_colors, color_avg_cases, color_avg_deaths, country_aqis, country_deaths_by_cases):
    fig = go.Figure(data=go.Scatter(x=country_names,
                                y=country_cases_by_pop,
                                mode='markers',
                                marker_color=country_colors
                                )) # hover text goes here
    fig.update_layout(title='Countries vs Cases/Population')
    fig.write_html('Countries_vs_CasesbyPopulation.html', auto_open=True)

    fig = go.Figure(data=go.Scatter(x=country_aqis,
                                y=country_cases_by_pop,
                                mode='markers',
                                marker_color=country_colors,
                                text = country_names
                                )) # hover text goes here
    fig.update_layout(title='US AQI vs Cases/Population', xaxis= dict(title = "US AQI"), yaxis = dict(title = "%Cases"))
    fig.write_html('US_AQI_vs_CasesbyPopulation.html', auto_open=True)

    fig = go.Figure(data=go.Scatter(x=country_aqis,
                                y=country_deaths_by_cases,
                                mode='markers',
                                marker_color=country_colors,
                                text = country_names
                                )) # hover text goes here
    fig.update_layout(title='US AQI vs Mortality Rate', xaxis= dict(title = "US AQI"), yaxis = dict(title = "Deaths/Cases"))
    fig.write_html('US_AQI_vs_Mortality_Rate.html', auto_open=True)


    colors = ["Green", "Yellow", "Orange", "Red"]
    
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=colors,
        y=color_avg_cases,
        marker_color= colors
    ))
    fig.update_layout(title='Average Cases by Air Quality Colors', xaxis= dict(title = "Country Air Quality"), yaxis = dict(title = "Average Cases"))
    fig.write_html('Average_Cases_by_Air_Quality_Colors.html', auto_open=True)

    print("Graphs created!")

def main():
    #get all data
    curr,conn = setUpDatabase("covid_data.db")
    curr.execute("SELECT * FROM CountryCases JOIN CountryAQIs ON CountryCases.name = CountryAQIs.name")
    list_all = curr.fetchall()
    # name cases deaths population LE lat lon | name aqi color
    #  0     1     2         3      4  5   6      7   8    9
    total_cases = 0
    num_elements = len(list_all)
    country_dict = {}
    total_population = 0
    total_deaths = 0
    total_aqi = 0
    aqi_color = ''
    list_all = sorted(list_all)

    #Graph data
    country_names = []
    country_cases_by_pop = []
    country_colors = []
    country_aqis = []
    country_deaths_by_cases = []
    for country_data in list_all:

        total_cases += country_data[1]
        total_population += country_data[3]
        total_deaths += country_data[2]
        total_aqi += country_data[8]
        aqi_color = country_data[9]

        cases_by_aqi = country_data[1]/country_data[8]
        cases_by_pop = country_data[1]/country_data[3]
        deaths_by_cases = country_data[2]/country_data[1]
        country_dict[country_data[0]] = (cases_by_aqi,cases_by_pop, deaths_by_cases, aqi_color)

        #Graph data:
        country_names.append(country_data[0])
        country_cases_by_pop.append(cases_by_pop)
        country_colors.append(country_data[9])
        country_aqis.append(country_data[8])
        country_deaths_by_cases.append(deaths_by_cases)
    
    color_data = aqiColorAverages(country_dict)
    color_avg_cases = color_data[0]
    color_avg_deaths = color_data[1]
    
    writeToFile(total_cases,total_deaths,total_population,total_aqi,color_avg_cases,color_avg_deaths,country_dict,num_elements)   

    createGraphs(country_names, country_cases_by_pop,country_colors, color_avg_cases, color_avg_deaths,country_aqis,country_deaths_by_cases)
    #get average number of deaths per country

if __name__ == "__main__":
    main()
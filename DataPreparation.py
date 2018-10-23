# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 10:51:01 2018

@author: Mononito Goswami
"""

import pandas

#Preparing the Electricity Demand Dataframe
path_Hourly_EDemand = r"..\PUB_DemandZonal_2017.csv" #Relative paths instead of absolute paths
fields = ['Date', 'Hour', 'Ontario Demand', 'Toronto']
dataframe_Hourly_EDemand = pandas.read_csv(path_Hourly_EDemand, skiprows = 3, usecols = fields)
#Skip the first few rows  

dataframe_Daily_EDemand_values = [0]*3
dataframe_Daily_EDemand_list = []
column_Headers = ['Date/Time', 'Ontario Demand', 'Toronto'] 

for row in range(dataframe_Hourly_EDemand.shape[0]):     
    dataframe_Daily_EDemand_values[1] = dataframe_Daily_EDemand_values[1] + float(dataframe_Hourly_EDemand.iloc[row, 2])
    #print (float(dataframe_Hourly_EDemand.iloc[row, 2]))
    
    dataframe_Daily_EDemand_values[2] = dataframe_Daily_EDemand_values[2] + float(dataframe_Hourly_EDemand.iloc[row, 3])
    
    if (float(dataframe_Hourly_EDemand.iloc[row, 1])%24 == 0): #If this is the last value of the day then:
        #dataframe_Daily_EDemand_values = [0]*3 #Is this memory efficient? 
        dataframe_Daily_EDemand_values[0] = dataframe_Hourly_EDemand.iloc[row, 0]
        #print (dataframe_Daily_EDemand_values)
        dataframe_Daily_EDemand_list.append(dataframe_Daily_EDemand_values)    
        #print (dataframe_Daily_EDemand_list)
        #Reinitializing values
        #for i in range(3): #Why wrong results? 
        #    dataframe_Daily_EDemand_values[i] = 0
        #dataframe_Daily_EDemand_values.clear()
        dataframe_Daily_EDemand_values = [0]*3

dataframe_Daily_EDemand = pandas.DataFrame(dataframe_Daily_EDemand_list, columns = column_Headers)

#Preparing the Weather Dataframe
path_Daily_Weather = r"..\eng-daily-01012017-12312017.csv"
fields_Weather = ['Date/Time', 'Year', 'Month', 'Day', 'Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)', 'Heat Deg Days (°C)', 'Total Rain (mm)', 'Total Snow (cm)', 'Snow on Grnd (cm)', 'Spd of Max Gust (km/h)']

dataframe_Daily_Weather = pandas.read_csv(path_Daily_Weather, skiprows = 24, usecols = fields_Weather)

dataframe = dataframe_Daily_EDemand.merge(dataframe_Daily_Weather, how = "inner", left_index = True, right_index = True, copy = False)
#Merge DataFrame objects by performing a database-style join operation by columns or indexes

dataframe = dataframe.drop(['Date/Time_x', 'Date/Time_y'], axis = 1)
#{0 or ‘index’, 1 or ‘columns’}

path = r"..\ElectricityDemandData_Uncleaned.csv"
dataframe.to_csv(path, sep = ',')

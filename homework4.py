# -*- coding: utf-8 -*-
"""
Created on Fri May 10 04:10:36 2024

@author: ChelseySSS
"""

# PPHA 30537
# Spring 2024
# Homework 4

# SHIHAN ZHAO

# SHIHAN ZHAO
# sz111111

# Due date: Sunday May 12th before midnight
# Write your answers in the space between the questions, and commit/push only
# this file to your repo. Note that there can be a difference between giving a
# "minimally" right answer, and a really good answer, so it can pay to put
# thought into your work.

##################

# Question 1: Explore the data APIs available from Pandas DataReader. Pick
# any two countries, and then 
#   a) Find two time series for each place
#      - The time series should have some overlap, though it does not have to
#        be perfectly aligned.
#      - At least one should be from the World Bank, and at least one should
#        not be from the World Bank.
#      - At least one should have a frequency that does not match the others,
#        e.g. annual, quarterly, monthly.
#      - You do not have to make four distinct downloads if it's more appropriate
#        to do a group of them, e.g. by passing two series titles to FRED.

import pandas_datareader.data as web
import pandas_datareader.wb as wb
from datetime import datetime

# Set the time period for the data
start = datetime(2000, 1, 1)
end = datetime(2020, 1, 1)

# For Japan
# World Bank: GDP per capita (annual)
japan_gdp = wb.download(indicator='NY.GDP.PCAP.CD', country=['JP'], start=start.year, end=end.year)

# FRED: Unemployment Rate (monthly)
japan_unemployment = web.DataReader('LRUN64TTJPM156S', 'fred', start, end)

# For Brazil
# World Bank: Total population (annual)
brazil_population = wb.download(indicator='SP.POP.TOTL', country=['BR'], start=start.year, end=end.year)

# FRED: Consumer Price Index (monthly)
brazil_cpi = web.DataReader('BRACPIALLMINMEI', 'fred', start, end)

# Display the data
print("Japan GDP Per Capita (World Bank):")
print(japan_gdp.head())
print("\nJapan Unemployment Rate (FRED):")
print(japan_unemployment.head())

print("\nBrazil Population (World Bank):")
print(brazil_population.head())
print("\nBrazil CPI (FRED):")
print(brazil_cpi.head())




#   b) Adjust the data so that all four are at the same frequency (you'll have
#      to look this up), then do any necessary merge and reshaping to put
#      them together into one long (tidy) format dataframe.

import pandas as pd

# Set the time period for the data
start = datetime(2000, 1, 1)
end = datetime(2020, 1, 1)

# Fetching the data
# For Japan
japan_gdp = wb.download(indicator='NY.GDP.PCAP.CD', country=['JP'], start=start.year, end=end.year)
japan_gdp.reset_index(inplace=True)
japan_gdp['year'] = pd.to_datetime(japan_gdp['year']).dt.year

japan_unemployment = web.DataReader('LRUN64TTJPM156S', 'fred', start, end)
japan_unemployment = japan_unemployment.resample('A').mean()
japan_unemployment.reset_index(inplace=True)
japan_unemployment['DATE'] = japan_unemployment['DATE'].dt.year
japan_unemployment.rename(columns={'DATE': 'year'}, inplace=True)

# For Brazil
brazil_population = wb.download(indicator='SP.POP.TOTL', country=['BR'], start=start.year, end=end.year)
brazil_population.reset_index(inplace=True)
brazil_population['year'] = pd.to_datetime(brazil_population['year']).dt.year

brazil_cpi = web.DataReader('BRACPIALLMINMEI', 'fred', start, end)
brazil_cpi = brazil_cpi.resample('A').mean()
brazil_cpi.reset_index(inplace=True)
brazil_cpi['DATE'] = brazil_cpi['DATE'].dt.year
brazil_cpi.rename(columns={'DATE': 'year'}, inplace=True)

# Renaming columns for clarity
japan_gdp.columns = ['Country', 'year', 'GDP_Per_Capita']
japan_unemployment.columns = ['year', 'Unemployment_Rate']
brazil_population.columns = ['Country', 'year', 'Total_Population']
brazil_cpi.columns = ['year', 'CPI']

# Merging data for each country
japan_data = pd.merge(japan_gdp, japan_unemployment, on='year', how='outer')
brazil_data = pd.merge(brazil_population, brazil_cpi, on='year', how='outer')

# Concatenating both country data
combined_data = pd.concat([japan_data, brazil_data])

# Reshaping to long format
tidy_data = pd.melt(combined_data, id_vars=['Country', 'year'], var_name='Indicator', value_name='Value')

# Display the tidy data
print(tidy_data.head())


# Fetching the data
# Brazil Total Population (World Bank)
brazil_population = wb.download(indicator='SP.POP.TOTL', country=['BR'], start=start.year, end=end.year)
brazil_population = brazil_population.reset_index()
brazil_population['year'] = pd.to_datetime(brazil_population['year'], format='%Y')  
brazil_population.rename(columns={'SP.POP.TOTL': 'Total_Population'}, inplace=True)

# Brazil Consumer Price Index (FRED)
brazil_cpi = web.DataReader('BRACPIALLMINMEI', 'fred', start, end).resample('A').mean()
brazil_cpi.reset_index(inplace=True)
brazil_cpi.rename(columns={'DATE': 'Year', 'BRACPIALLMINMEI': 'CPI'}, inplace=True)

# Ensure both data frames have the same 'Year' format
brazil_population['Year'] = brazil_population['year'].dt.to_period('A').dt.to_timestamp()  
brazil_cpi['Year'] = brazil_cpi['Year'].dt.to_period('A').dt.to_timestamp()

# Dropping extra 'year' column from population data
brazil_population.drop('year', axis=1, inplace=True)

# Merging data
brazil_data = pd.merge(brazil_population, brazil_cpi, on='Year', how='outer')

# Adding country label
brazil_data['Country'] = 'Brazil'

# Reshaping to long format
brazil_tidy = pd.melt(brazil_data, id_vars=['Country', 'Year'], var_name='Indicator', value_name='Value')

# Display the tidy data
print(brazil_tidy.head())




#   c) Finally, go back and change your earlier code so that the
#      countries and dates are set in variables at the top of the file. Your
#      final result for parts a and b should allow you to (hypothetically) 
#      modify these values easily so that your code would download the data
#      and merge for different countries and dates.
#      - You do not have to leave your code from any previous way you did it
#        in the file. If you did it this way from the start, congrats!
#      - You do not have to account for the validity of all the possible 
#        countries and dates, e.g. if you downloaded the US and Canada for 
#        1990-2000, you can ignore the fact that maybe this data for some
#        other two countries aren't available at these dates.

# Variables for easy modification
countries = ['BR', 'JP']  
start_date = datetime(2000, 1, 1)
end_date = datetime(2020, 1, 1)

# Data fetching functions
def fetch_world_bank_data(indicator, country, start_year, end_year):
    data = wb.download(indicator=indicator, country=[country], start=start_year, end=end_year)
    data = data.reset_index()
    data['year'] = pd.to_datetime(data['year'], format='%Y')  
    data.rename(columns={indicator: 'Value'}, inplace=True)
    data['Year'] = data['year'].dt.to_period('A').dt.to_timestamp()
    data.drop('year', axis=1, inplace=True)
    return data

def fetch_fred_data(series_id, start, end, column_name):
    data = web.DataReader(series_id, 'fred', start, end).resample('A').mean()
    data.reset_index(inplace=True)
    data.rename(columns={series_id: 'Value', 'DATE': 'Year'}, inplace=True)
    return data

# Fetching and merging data for each country
all_data = []

for country in countries:
    if country == 'BR':
        population = fetch_world_bank_data('SP.POP.TOTL', 'BR', start_date.year, end_date.year)
        population['Indicator'] = 'Total_Population'
        cpi = fetch_fred_data('BRACPIALLMINMEI', start_date, end_date, 'CPI')
        cpi['Indicator'] = 'CPI'
        country_data = pd.concat([population, cpi])
        country_data['Country'] = 'Brazil'
    elif country == 'JP':
        gdp = fetch_world_bank_data('NY.GDP.PCAP.CD', 'JP', start_date.year, end_date.year)
        gdp['Indicator'] = 'GDP_Per_Capita'
        unemployment = fetch_fred_data('LRUN64TTJPM156S', start_date, end_date, 'Unemployment_Rate')
        unemployment['Indicator'] = 'Unemployment_Rate'
        country_data = pd.concat([gdp, unemployment])
        country_data['Country'] = 'Japan'
    
    all_data.append(country_data)

# Concatenating both country data
combined_data = pd.concat(all_data)

# Display the tidy data
print(combined_data.head())




#   d) Clean up any column names and values so that the data is consistent
#      and clear, e.g. don't leave some columns named in all caps and others
#      in all lower-case, or some with unclear names, or a column of mixed 
#      strings and integers. Write the dataframe you've created out to a 
#      file named q1.csv, and commit it to your repo.

# Variables for easy modification
countries = ['BR', 'JP']  
start_date = datetime(2000, 1, 1)
end_date = datetime(2020, 1, 1)

# Data fetching functions
def fetch_world_bank_data(indicator, country, start_year, end_year):
    data = wb.download(indicator=indicator, country=[country], start=start_year, end=end_year)
    data = data.reset_index()
    data['year'] = pd.to_datetime(data['year'], format='%Y')  
    data.rename(columns={indicator: 'value'}, inplace=True)
    data['year'] = data['year'].dt.to_period('A').dt.to_timestamp()
    return data[['country', 'year', 'value']]

def fetch_fred_data(series_id, start, end, name):
    data = web.DataReader(series_id, 'fred', start, end).resample('A').mean()
    data.reset_index(inplace=True)
    data.rename(columns={series_id: 'value', 'DATE': 'year'}, inplace=True)
    return data

# Fetching and merging data for each country
all_data = []

for country in countries:
    if country == 'BR':
        population = fetch_world_bank_data('SP.POP.TOTL', 'BR', start_date.year, end_date.year)
        population['indicator'] = 'Total Population'
        cpi = fetch_fred_data('BRACPIALLMINMEI', start_date, end_date, 'Consumer Price Index')
        cpi['indicator'] = 'Consumer Price Index'
        country_data = pd.concat([population, cpi])
        country_data['country'] = 'Brazil'
    elif country == 'JP':
        gdp = fetch_world_bank_data('NY.GDP.PCAP.CD', 'JP', start_date.year, end_date.year)
        gdp['indicator'] = 'GDP Per Capita'
        unemployment = fetch_fred_data('LRUN64TTJPM156S', start_date, end_date, 'Unemployment Rate')
        unemployment['indicator'] = 'Unemployment Rate'
        country_data = pd.concat([gdp, unemployment])
        country_data['country'] = 'Japan'
    
    all_data.append(country_data)

# Concatenating both country data
combined_data = pd.concat(all_data)

# Cleaning column names
combined_data.columns = ['country', 'year', 'value', 'indicator']
combined_data['year'] = combined_data['year'].dt.year  

# Saving to CSV
combined_data.to_csv('q1.csv', index=False)

# Display message when done
print("Data has been cleaned and saved to q1.csv.")




# Question 2: On the following Harris School website:
# https://harris.uchicago.edu/academics/design-your-path/certificates/certificate-data-analytics
# There is a list of six bullet points under "Required courses" and 12
# bullet points under "Elective courses". Using requests and BeautifulSoup: 
#   - Collect the text of each of these bullet points
#   - Add each bullet point to the csv_doc list below as strings (following 
#     the columns already specified). The first string that gets added should be 
#     approximately in the form of: 
#     'required,PPHA 30535 or PPHA 30537 Data and Programming for Public Policy I'
#   - Hint: recall that \n is the new-line character in text
#   - You do not have to clean up the text of each bullet point, or split the details out
#     of it, like the course code and course description, but it's a good exercise to
#     think about.
#   - Using context management, write the data out to a file named q2.csv
#   - Finally, import Pandas and test loading q2.csv with the read_csv function.
#     Use asserts to test that the dataframe has 18 rows and two columns.

import requests
from bs4 import BeautifulSoup
import pandas as pd

# The URL
url = 'https://harris.uchicago.edu/academics/design-your-path/certificates/certificate-data-analytics'

# Make a request to the webpage
response = requests.get(url)
response.raise_for_status()  

# Parse the HTML content
soup = BeautifulSoup(response.text, 'html.parser')

# Initialize the csv_doc list with column headers
csv_doc = ['type,description']

# Function to fetch courses under specified section
def fetch_courses(header_text):
    header = soup.find(lambda tag: tag.name == "h3" and header_text in tag.text)
    if header:
        ul = header.find_next_sibling('ul')
        if ul:
            for li in ul.find_all('li'):
                if 'Required courses' in header_text:
                    course_type = 'required'
                else:
                    course_type = 'elective'
                # Extract text and construct CSV format string
                csv_doc.append(f"{course_type},{li.text.strip().replace(',', ';')}")

# Fetch required and elective courses
fetch_courses('Required courses')
fetch_courses('Elective courses')

# Writing to a CSV file
with open('q2.csv', 'w') as file:
    for line in csv_doc:
        file.write(line + '\n')

# Load the data using pandas to test it
df = pd.read_csv('q2.csv')
assert len(df) == 18, "There should be 18 rows in the DataFrame."
assert len(df.columns) == 2, "There should be two columns in the DataFrame."

# Print DataFrame to check the loaded data
print(df)







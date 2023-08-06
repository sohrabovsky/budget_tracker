#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug  5 16:05:16 2023

@author: sohrab-salehin
"""
# In this script, sales of 3 months for dom. Hotel, Dom. Flight, Bus, Int. Flight, Int. Hotel, and Train
# are imported and then by using ARIMA model, we can have a forecast of overal performance of each vertical
# you should answer the machine, how many days you want to forecast for verticals
# Also you should specify date from and date to of sales to be used for forecasting
# Note that for Train, you should keep in mind days of pre sales because it causes
# spikes in sales trend which wont be reflected in forecast charts


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.forecasting.stl import STLForecast
from statsmodels.tsa.arima.model import ARIMA

sns.set_theme()

number_of_forecasting_days = int(input('Please specify number of days for forecasting: '))
tracker_first_date= input('sales data for analyzing ARIMA from date: ')
tracker_last_date= input('sales data for analyzing ARIMA to date: ')


# Table for having overal forecasts:
    
    
index= ['dom. Hotel', 'dom. Flight', 'Bus', 'Int. Flight', 'Int. Hotel', 'Train']
columns= ['Forecasted tickets/roomnights']
table= pd.DataFrame(index= index, columns= columns)

# dom. Hotel

hotel = pd.read_csv(
    "hotel_sales.csv",
    index_col=["Registered Date"],
    parse_dates=True,
    dtype={"Booking ID": str},
).rename(
    columns={
        "Registered Date": "date",
        "Sum of New Etl Room Nights": "item",
    }
)

hotel_sales = pd.pivot_table(
    hotel, index=hotel.index, values="item", aggfunc={"item": "sum"}
)
hotel_sales.index.freq = pd.infer_freq(hotel_sales.index)

stlf = STLForecast(hotel_sales, ARIMA, model_kwargs=dict(order=(1, 1, 0), trend="t"))
stlf_res = stlf.fit()
forecast = stlf_res.forecast(number_of_forecasting_days)
hotel_forecast = np.sum(
    hotel_sales[hotel_sales.index >= tracker_first_date].values
) + np.sum(forecast.values)

fig, ax = plt.subplots(1, 1, figsize=(12, 6))
ax.plot(np.array(hotel_sales.index), np.array(hotel_sales.values), color="b")
ax.plot(np.array(forecast.index), np.array(forecast.values), color="r")
plt.title("dom. Hotel Forecast")
plt.xlabel("Dates")
plt.ylabel("Room Nights")
plt.show()
table.loc['dom. Hotel', 'Forecasted tickets/roomnights']

# dom. Flight

flight = pd.read_csv(
    "flight_sales.csv",
    index_col=["Paid Date"],
    parse_dates=True,
    dtype={"Distinct values of Ticket ID": int},
).rename(
    columns={
        "Paid Date": "date",
        "Distinct values of Ticket ID": "item",
    }
)
flight = flight[flight.index >= "2023-06-07"]

flight_sales = pd.pivot_table(
    flight, index=flight.index, values="item", aggfunc={"item": "sum"}
)
flight_sales.index.freq = pd.infer_freq(flight_sales.index)

stlf = STLForecast(flight_sales, ARIMA, model_kwargs=dict(order=(1, 1, 0), trend="t"))
stlf_res = stlf.fit()
forecast = stlf_res.forecast(number_of_forecasting_days)
flight_forecast = int(
    np.sum(flight_sales[flight_sales.index >= tracker_first_date].values)
) + int(np.sum(forecast.values))

fig, ax = plt.subplots(1, 1, figsize=(12, 6))
ax.plot(
    np.array(flight_sales.index),
    np.array(flight_sales.values),
    color="b",
    label="Actual",
)
ax.plot(
    np.array(forecast.index), np.array(forecast.values), color="r", label="Forecast"
)

plt.title("dom. Flight Forecast")
plt.xlabel("Dates")
plt.ylabel("Tickets")
plt.legend()
plt.show()
table.loc['dom. Flight', 'Forecasted tickets/roomnights']

# Bus

bus = pd.read_csv(
    "bus_sales.csv",
    index_col="Pay At",
    parse_dates=True,
).rename(
    columns={
        "Book Created At": "date",
        "Sum of Number Of Seats Per Book": "item",
    }
)
bus_sales = pd.pivot_table(bus, index=bus.index, values="item", aggfunc={"item": "sum"})
bus_sales.index.freq = pd.infer_freq(bus_sales.index)

stlf = STLForecast(bus_sales, ARIMA, model_kwargs=dict(order=(1, 1, 0), trend="t"))
stlf_res = stlf.fit()
forecast = stlf_res.forecast(number_of_forecasting_days)
bus_forecast = np.sum(bus_sales[bus_sales.index >= tracker_first_date].values) + np.sum(
    forecast.values
)

fig, ax = plt.subplots(1, 1, figsize=(12, 6))
ax.plot(np.array(bus_sales.index), np.array(bus_sales.values), color="b")
ax.plot(np.array(forecast.index), np.array(forecast.values), color="r")
plt.title("Bus Forecast")
plt.xlabel("Dates")
plt.ylabel("Tickets")
plt.show()
table.loc['Bus', 'Forecasted tickets/roomnights']

# Int. Flight

intflight = pd.read_csv(
    "intflight_sales.csv",
    index_col=["Booking Date"],
    parse_dates=True,
).rename(columns={"Booking Date": "date", "Count": "item"})

intflight_sales = pd.pivot_table(
    intflight, index=intflight.index, values="item", aggfunc={"item": "sum"}
)
intflight_sales.index.freq = pd.infer_freq(intflight_sales.index)

stlf = STLForecast(
    intflight_sales, ARIMA, model_kwargs=dict(order=(1, 1, 0), trend="t")
)
stlf_res = stlf.fit()
forecast = stlf_res.forecast(number_of_forecasting_days)
intflight_forecast = np.sum(
    intflight_sales[intflight_sales.index >= tracker_first_date].values
) + np.sum(forecast.values)

fig, ax = plt.subplots(1, 1, figsize=(12, 6))
ax.plot(np.array(intflight_sales.index), np.array(intflight_sales.values), color="b")
ax.plot(np.array(forecast.index), np.array(forecast.values), color="r")
plt.title("Intflight Forecast")
plt.xlabel("Dates")
plt.ylabel("Tickets")
plt.show()
table.loc['Int. Flight', 'Forecasted tickets/roomnights']

# Int. Hotel

inthotel = pd.read_csv(
    "inthotel_sales.csv",
    index_col=["Registered Date"],
    parse_dates=True,
).rename(
    columns={
        "Sum of Room Nights": "item",
    }
)

inthotel.index.freq = pd.infer_freq(inthotel.index)

stlf = STLForecast(inthotel, ARIMA, model_kwargs=dict(order=(1, 1, 0), trend="t"))
stlf_res = stlf.fit()
forecast = stlf_res.forecast(number_of_forecasting_days)
inthotel_forecast = np.sum(
    inthotel[inthotel.index >= tracker_first_date].values
) + np.sum(forecast.values)

fig, ax = plt.subplots(1, 1, figsize=(12, 6))
ax.plot(np.array(inthotel.index), np.array(inthotel.values), color="b")
ax.plot(np.array(forecast.index), np.array(forecast.values), color="r")
plt.title("Int-Hotel Forecast")
plt.xlabel("Dates")
plt.ylabel("Room Nights")
plt.show()
table.loc['Int. Hotel', 'Forecasted tickets/roomnights']

# Train

train = pd.read_csv(
    "train_sales.csv",
    parse_dates=True,
    index_col=["Paid At"],
).rename(
    columns={
        "Paid At": "date",
        "count_seat": "item",
    }
)
train.index.freq = pd.infer_freq(train.index)

stlf = STLForecast(train, ARIMA, model_kwargs=dict(order=(1, 1, 0), trend="t"))
stlf_res = stlf.fit()
forecast = stlf_res.forecast(number_of_forecasting_days)
train_forecast = np.sum(train[train.index >= tracker_first_date].values) + np.sum(
    forecast.values
)

fig, ax = plt.subplots(1, 1, figsize=(12, 6))
ax.plot(np.array(train.index), np.array(train.values), color="b")
ax.plot(np.array(forecast.index), np.array(forecast.values), color="r")
plt.title("Train Forecast")
plt.xlabel("Dates")
plt.ylabel("Tickets")
plt.show()
table.loc['Train', 'Forecasted tickets/roomnights']


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul  1 17:34:35 2023

@author: sohrab-salehin
"""

import pandas as pd
import numpy as np

import gspread
from oauth2client.service_account import ServiceAccountCredentials

path = r"/home/sohrab-salehin/Documents/python_scripts/GitHub/budget_tracker/"

# Importing files


# This block of code is for reading Spending sheet of marketing for non voucher spendings

# Replace 'your-credentials.json' with the actual name of your credentials JSON file
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    path + "budgettracker-381004-e2e7eb7a8a7a.json",
    scope,
)
client = gspread.authorize(creds)

sh = client.open("Marketing Spending ST")
worksheet = sh.worksheet("Data")

marketing_spending = worksheet.get_values()
marketing_spending = pd.DataFrame(marketing_spending)
headers = marketing_spending.iloc[0]
marketing_spending.columns = headers
marketing_spending = marketing_spending[1:]
marketing_spending.reset_index(drop=True, inplace=True)
marketing_spending.reset_index(drop=True, inplace=True)

marketing_spending.rename(
    columns={"Action_In_Channels": "action_in_channels", "Date": "date"}, inplace=True
)
marketing_spending.dropna(subset="date", inplace=True)
# Your previous code here...

# Remove commas from the "Cost_TT" column and replace empty strings with NaN
marketing_spending["Cost_TT"] = (
    marketing_spending["Cost_TT"].str.replace(",", "").replace("", np.nan)
)

# Drop rows with NaN in the "Cost_TT" column
marketing_spending.dropna(subset=["Cost_TT"], inplace=True)

# Convert "Cost_TT" column to float

marketing_spending["Cost_TT"] = marketing_spending["Cost_TT"].astype(float)

############################################################ Importing files ######################################################
channel_defenition= pd.read_excel(path +'channel_defenition.xlsx')      # for converting action in channels of maketing spending sheet to BP report
date_from= input("Enter from date: ")
date_to= input("Enter to date: ")


marketing_spending = marketing_spending.iloc[:, :6]
marketing_spending = marketing_spending.merge(channel_defenition, on= 'action_in_channels', how= 'inner')
marketing_spending = marketing_spending[
    (marketing_spending["date"] >= date_from) & (marketing_spending["date"] <= date_to)
].sort_values(by="date")
marketing_spending['Cost_TT']= marketing_spending['Cost_TT']*10

table_rows= ['adwords', 'loyalty', 'crm' ,'campaign', 'affiliate','cross_sell','other_voucher','other', 'pr', 'sms','social','display', 'seo','offline', 'total']
table_columns= ['BDG', 'orders', 'tickets', 'cpo', 'cpt', 'os']

big_table= pd.DataFrame()   # Big table is output of this code which is actuals of budget tracker for all verticals

################################################## Dom. Hotel ############################################
hotel = pd.read_csv(
    path + "hotel.csv",
    parse_dates=["Registered Date"],
    dtype={"Booking ID": str, "Mobile": str, "discount_code": str, "Utm Campaign": str},
).rename(
    columns={
        "Registered Date": "date",
        "Booking ID": "invoiceID",
        "Tag": "tag",
        "Discount Code": "discount_code",
        "Utm Campaign": "utm_campaign",
        "Channel": "channel",
        "Room Nights": "item",
        "Discount Code Value": "discount_amount",
    }
)

hotel_channel_b2c = [
    "B2C - Domestic",
    "SnappJek",
    "AP",
    "SnappTrip_Mobile_App",
    "web_app",
    "Myirancell",
    "Telesales",
]
hotel_b2c = hotel[hotel["channel"].isin(hotel_channel_b2c)].reset_index(drop=True)
hotel_b2c = hotel_b2c[
    (hotel_b2c["date"] >= date_from) & (hotel_b2c["date"] <= date_to)
]
hotel_b2c['discount_code']= hotel_b2c['discount_code'].str.lower()
hotel_b2c['discount_amount']= hotel_b2c['discount_amount']*10

hotel_non_voucher = hotel_b2c[hotel_b2c.loc[:, "discount_code"].isna()]


index= hotel_b2c[hotel_b2c['tag'].str.contains('loyalty', case= False, na= False)].index
hotel_b2c.loc[index, 'marketing_channel']= 'loyalty'

index= hotel_b2c[
    (hotel_b2c['tag'].str.contains('journey', case= False)) & (~hotel_b2c["tag"].str.contains("cross", case=False, na=False)) 
    ].index
hotel_b2c.loc[index, 'marketing_channel']= 'crm'

index= hotel_b2c[
    hotel_b2c['tag'].str.contains('campaign', case= False, na= False)
    ].index
hotel_b2c.loc[index, 'marketing_channel']= 'campaign'

index= hotel_b2c[
    hotel_b2c['tag'].str.contains('cross', case= False, na= False)
    ].index
hotel_b2c.loc[index, 'marketing_channel']= 'cross_sell'

index1= hotel_b2c[
    (hotel_b2c['tag'].str.contains('guarantee', case= False, na= False))
    & (hotel_b2c['utm_campaign'].dropna().str.isdigit())
    & (
       ~hotel_b2c['tag'].str.contains(
           'affiliate-guarantee', case= False, na= False
           )
       )
    ].index
hotel_b2c.loc[index1, 'marketing_channel']= 'guarantee_adwords'

index2= hotel_b2c[
    (hotel_b2c['tag'].str.contains('guarantee', case= False, na= False))
    & (~hotel_b2c.index.isin(index1))
    ].index
hotel_b2c.loc[index2, 'marketing_channel']= 'guarantee_organic'

index3= hotel_b2c[
    hotel_b2c['tag'].str.contains('affiliate-guarantee', case= False, na= False)
    ].index
hotel_b2c.loc[index3, 'marketing_channel']= 'guarantee_affiliate'

index4= hotel_b2c[
    (hotel_b2c['tag'].str.contains('Affiliate', case= False, na= False))
    & (~hotel_b2c.index.isin(index3))
    ].index
hotel_b2c.loc[index4, 'marketing_channel']= 'affiliate'

utm_campaign = hotel_non_voucher.dropna(subset=["utm_campaign"])
utm_campaign = utm_campaign[utm_campaign["utm_campaign"].str.isdigit()]
index5 = pd.concat(
    [pd.Series(index1), pd.Series(utm_campaign.index)]
)
hotel_adwords = hotel_b2c[hotel_b2c.index.isin(index5)]  ### Based on Metabase

hotel_adwords_orders= int(input("Hotel adwords orders: "))
room_night_order = hotel_adwords["item"].sum() / hotel_adwords["invoiceID"].nunique()
adwords_room_nights = (
    hotel_adwords_orders * room_night_order
)  ### Based on Adwords Pannel

index6= hotel_b2c[(hotel_b2c['discount_code'].notna()) & (hotel_b2c['marketing_channel'].isna())].index
hotel_b2c.loc[index6, 'marketing_channel']= 'other_voucher'

index7= hotel_b2c[hotel_b2c['marketing_channel'].isna()].index
hotel_b2c.loc[index7, 'marketing_channel']= 'organic'

# Costs

hotel_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Domestic Hotel']



table= pd.DataFrame(index= table_rows, columns= table_columns)

table.loc['adwords', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'adwords']['Cost_TT'].sum()

df= hotel_b2c[hotel_b2c['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'BDG']= float(df['discount_amount'].sum())

df= hotel_b2c[hotel_b2c['marketing_channel'] == 'crm']
table.loc['crm', 'BDG']= float(df['discount_amount'].sum())

df= hotel_b2c[hotel_b2c['marketing_channel'] == 'campaign']
table.loc['campaign', 'BDG']= float(df['discount_amount'].sum())

df= hotel_b2c[hotel_b2c['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'BDG']= (
                                float(df['discount_amount'].sum()) + 
                                hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'affiliate']['Cost_TT'].sum()
                                )
df= hotel_b2c[hotel_b2c['marketing_channel'] == 'cross_sell']
table.loc['cross_sell', 'BDG']= float(df['discount_amount'].sum())

df= hotel_b2c[hotel_b2c['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'BDG']= float(df['discount_amount'].sum())
    
table.loc['display', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= hotel_marketing_costs[hotel_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()


table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()   ### Needs to be corrected

# Orders

table.loc['adwords', 'orders']= hotel_adwords_orders
table.loc['loyalty', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'loyalty']['invoiceID'].nunique()
table.loc['crm', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'crm']['invoiceID'].nunique()
table.loc['campaign', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'campaign']['invoiceID'].nunique()
table.loc['affiliate', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'affiliate']['invoiceID'].nunique()
table.loc['cross_sell', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'cross_sell']['invoiceID'].nunique()
table.loc['other_voucher', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'other_voucher']['invoiceID'].nunique()

table.loc['display', 'orders']= 0
table.loc['sms', 'orders']= 0
table.loc['seo', 'orders']= 0
table.loc['pr', 'orders']= 0
table.loc['social', 'orders']= 0
table.loc['other', 'orders']= hotel_b2c[hotel_b2c['marketing_channel'] == 'organic']['invoiceID'].nunique()
table.loc['total', 'orders']= hotel_b2c['invoiceID'].nunique()


# Roomnights

table.loc['loyalty', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'loyalty']['item'].sum()
table.loc['crm', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'crm']['item'].sum()
table.loc['campaign', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'campaign']['item'].sum()
table.loc['affiliate', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'affiliate']['item'].sum()
table.loc['cross_sell', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'cross_sell']['item'].sum()
table.loc['other_voucher', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'other_voucher']['item'].sum()

table.loc['display', 'tickets']= 0
table.loc['sms', 'tickets']= 0
table.loc['seo', 'tickets']= 0
table.loc['pr', 'tickets']= 0
table.loc['social', 'tickets']= 0
table.loc['other', 'tickets']= hotel_b2c[hotel_b2c['marketing_channel'] == 'organic']['item'].sum()
table.loc['total', 'tickets']= hotel_b2c['item'].sum()
table.loc['adwords', 'tickets']= adwords_room_nights

# CPO
table.loc['adwords', 'cpo']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'orders']
table.loc['loyalty', 'cpo']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'orders']
table.loc['crm', 'cpo']= table.loc['crm', 'BDG'] / table.loc['crm', 'orders']
table.loc['campaign', 'cpo']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'orders']
table.loc['affiliate', 'cpo']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'orders']
try:
    table.loc['other_voucher', 'cpo']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'orders']
except:
    table.loc['other_voucher', 'cpo']= 0
table.loc['cross_sell', 'cpo']= table.loc['cross_sell', 'BDG'] / table.loc['cross_sell', 'orders']
table.loc['display', 'cpo']= 0
table.loc['sms', 'cpo']= 0
table.loc['seo', 'cpo']= 0
table.loc['pr', 'cpo']= 0
table.loc['social', 'cpo']= 0
table.loc['other', 'cpo']= 0
table.loc['total', 'cpo']= (table.loc['total', 'BDG'] - table.loc['offline', 'BDG']) / table.loc['total', 'orders']

# CPT
table.loc['adwords', 'cpt']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'tickets']
table.loc['loyalty', 'cpt']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'tickets']
table.loc['crm', 'cpt']= table.loc['crm', 'BDG'] / table.loc['crm', 'tickets']
table.loc['campaign', 'cpt']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'tickets']
table.loc['affiliate', 'cpt']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'tickets']
table.loc['cross_sell', 'cpt']= table.loc['cross_sell', 'BDG'] / table.loc['cross_sell', 'tickets']
table.loc['other_voucher', 'cpt']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'tickets']

table.loc['display', 'cpt']= 0
table.loc['sms', 'cpt']= 0
table.loc['seo', 'cpt']= 0
table.loc['pr', 'cpt']= 0
table.loc['social', 'cpt']= 0
table.loc['other', 'cpt']= 0
table.loc['total', 'cpt']= (table.loc['total', 'BDG'] - table.loc['offline', 'BDG']) / table.loc['total', 'tickets']

# OS
table.loc['adwords', 'os']= table.loc['adwords', 'orders'] / table.loc['total', 'orders']
table.loc['loyalty', 'os']= table.loc['loyalty', 'orders'] / table.loc['total', 'orders']
table.loc['crm', 'os']= table.loc['crm', 'orders'] / table.loc['total', 'orders']
table.loc['campaign', 'os']= table.loc['campaign', 'orders'] / table.loc['total', 'orders']
table.loc['affiliate', 'os']= table.loc['affiliate', 'orders'] / table.loc['total', 'orders']
table.loc['cross_sell', 'os']= table.loc['cross_sell', 'orders'] / table.loc['total', 'orders']
table.loc['other_voucher', 'os']= table.loc['other_voucher', 'orders'] / table.loc['total', 'orders']

table.loc['display', 'os']= 0
table.loc['sms', 'os']= 0
table.loc['seo', 'os']= 0
table.loc['pr', 'os']= 0
table.loc['social', 'os']= 0
table.loc['other', 'os']= table.loc['other', 'orders'] / table.loc['total', 'orders']
table.loc['total', 'os']= table.loc['total', 'orders'] / table.loc['total', 'orders']

table.loc[:,'vertical'] = 'hotel'
big_table= pd.concat([big_table, table], axis= 0)

################################################## Dom. Flight ##################################################

flight = pd.read_csv(
    path + "flight.csv",
    parse_dates=["created_date"],
    dtype={"invoice_id": str, "mobile": str, "utm_campaign": str},
).rename(
    columns={
        "created_date": "date",
        "created_date": "date",
        "invoice_id": "invoiceID",
        "discount_code_tag": "tag",
        "Utm Campaign": "utm_campaign",
        "Channel": "channel",
        "ticket_count": "item",
        "is_new_user_total": "new_vs_returning",
    }
)
flight = flight[flight["invoice_status"] == "issue-succeeded"]

flight_channel_b2c = [
    "Snapp",
    "SnappTrip_App",
    "SnappTrip_Website",
    "Irancell",
    "Unknown",
]
flight_b2c = flight[flight["channel"].isin(flight_channel_b2c)].reset_index(drop=True)
flight_b2c = flight_b2c[
    (flight_b2c["date"] >= date_from)
    & (flight_b2c["date"] <= date_to)
]
flight_b2c['discount_code']= flight_b2c['discount_code'].str.lower()

index= flight_b2c[flight_b2c['tag'].str.contains('loyalty', case= False, na= False)].index
flight_b2c.loc[index, 'marketing_channel']= 'loyalty'

index= flight_b2c[
    (flight_b2c['tag'].str.contains('journey', case= False, na= False)) 
    ].index
flight_b2c.loc[index, 'marketing_channel']= 'crm'

index= flight_b2c[
    flight_b2c['tag'].str.contains('campaign', case= False, na= False)
    ].index
flight_b2c.loc[index, 'marketing_channel']= 'campaign'

index= flight_b2c[
    (flight_b2c['tag'].str.contains('Affiliate', case= False, na= False))
        ].index
flight_b2c.loc[index, 'marketing_channel']= 'affiliate'

utm_campaign = flight_b2c.dropna(subset=["utm_campaign"])
utm_campaign = utm_campaign[utm_campaign["utm_campaign"].str.isdigit()]
index = utm_campaign.index
flight_adwords = flight_b2c[flight_b2c.index.isin(index)]  ### Based on Metabase
ticket_order = flight_adwords["item"].sum() / flight_adwords["invoiceID"].nunique()


flight_adwords_orders= int(input("Flight adwords orders: "))
ticket_order = flight_adwords["item"].sum() / flight_adwords["invoiceID"].nunique()
flight_adwords_tickets = (
    flight_adwords_orders * ticket_order
)  ### Based on Adwords Pannel
index= flight_b2c[(flight_b2c['discount_code'].notna()) & (flight_b2c['marketing_channel'].isna())].index
flight_b2c.loc[index, 'marketing_channel']= 'other_voucher'

index= flight_b2c[flight_b2c['marketing_channel'].isna()].index
flight_b2c.loc[index, 'marketing_channel']= 'organic'

# Costs

flight_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Domestic Flight']

table= pd.DataFrame(index= table_rows, columns= table_columns)

table.loc['adwords', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'adwords']['Cost_TT'].sum()

df= flight_b2c[flight_b2c['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'BDG']= float(df['discount_amount'].sum())

df= flight_b2c[flight_b2c['marketing_channel'] == 'crm']
table.loc['crm', 'BDG']= float(df['discount_amount'].sum())

df= flight_b2c[flight_b2c['marketing_channel'] == 'campaign']
table.loc['campaign', 'BDG']= float(df['discount_amount'].sum())

df= flight_b2c[flight_b2c['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'BDG']= (
                                float(df['discount_amount'].sum()) + 
                                flight_marketing_costs[flight_marketing_costs['channel_type'] == 'affiliate']['Cost_TT'].sum()
                                )

df= flight_b2c[flight_b2c['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'BDG']= float(df['discount_amount'].sum())
    
table.loc['display', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= flight_marketing_costs[flight_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()

table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()   ### Needs to be corrected

# Orders

table.loc['adwords', 'orders']= flight_adwords_orders
table.loc['loyalty', 'orders']= flight_b2c[flight_b2c['marketing_channel'] == 'loyalty']['invoiceID'].nunique()
table.loc['crm', 'orders']= flight_b2c[flight_b2c['marketing_channel'] == 'crm']['invoiceID'].nunique()
table.loc['campaign', 'orders']= flight_b2c[flight_b2c['marketing_channel'] == 'campaign']['invoiceID'].nunique()
table.loc['affiliate', 'orders']= flight_b2c[flight_b2c['marketing_channel'] == 'affiliate']['invoiceID'].nunique()
table.loc['other_voucher', 'orders']= flight_b2c[flight_b2c['marketing_channel'] == 'other_voucher']['invoiceID'].nunique()

table.loc['display', 'orders']= 0
table.loc['sms', 'orders']= 0
table.loc['seo', 'orders']= 0
table.loc['pr', 'orders']= 0
table.loc['social', 'orders']= 0
table.loc['other', 'orders']= flight_b2c[flight_b2c['marketing_channel'] == 'organic']['invoiceID'].nunique()
table.loc['total', 'orders']= flight_b2c['invoiceID'].nunique()


# Tickets

table.loc['loyalty', 'tickets']= flight_b2c[flight_b2c['marketing_channel'] == 'loyalty']['item'].sum()
table.loc['crm', 'tickets']= flight_b2c[flight_b2c['marketing_channel'] == 'crm']['item'].sum()
table.loc['campaign', 'tickets']= flight_b2c[flight_b2c['marketing_channel'] == 'campaign']['item'].sum()
table.loc['affiliate', 'tickets']= flight_b2c[flight_b2c['marketing_channel'] == 'affiliate']['item'].sum()
table.loc['other_voucher', 'tickets']= flight_b2c[flight_b2c['marketing_channel'] == 'other_voucher']['item'].sum()

table.loc['display', 'tickets']= 0
table.loc['sms', 'tickets']= 0
table.loc['seo', 'tickets']= 0
table.loc['pr', 'tickets']= 0
table.loc['social', 'tickets']= 0
table.loc['other', 'tickets']= flight_b2c[flight_b2c['marketing_channel'] == 'organic']['item'].sum()
table.loc['total', 'tickets']= flight_b2c['item'].sum()
table.loc['adwords', 'tickets']= flight_adwords_tickets

# CPO
table.loc['adwords', 'cpo']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'orders']
table.loc['loyalty', 'cpo']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'orders']
table.loc['crm', 'cpo']= table.loc['crm', 'BDG'] / table.loc['crm', 'orders']
table.loc['campaign', 'cpo']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'orders']
table.loc['affiliate', 'cpo']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'orders']
try:
    table.loc['other_voucher', 'cpo']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'orders']
except:
    table.loc['other_voucher', 'cpo']= 0
table.loc['display', 'cpo']= 0
table.loc['sms', 'cpo']= 0
table.loc['seo', 'cpo']= 0
table.loc['pr', 'cpo']= 0
table.loc['social', 'cpo']= 0
table.loc['other', 'cpo']= 0
table.loc['total', 'cpo']= (table.loc['total', 'BDG'] - table.loc['offline', 'BDG']) / table.loc['total', 'orders']

# CPT
table.loc['adwords', 'cpt']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'tickets']
table.loc['loyalty', 'cpt']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'tickets']
table.loc['crm', 'cpt']= table.loc['crm', 'BDG'] / table.loc['crm', 'tickets']
table.loc['campaign', 'cpt']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'tickets']
table.loc['affiliate', 'cpt']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'tickets']
table.loc['other_voucher', 'cpt']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'tickets']

table.loc['display', 'cpt']= 0
table.loc['sms', 'cpt']= 0
table.loc['seo', 'cpt']= 0
table.loc['pr', 'cpt']= 0
table.loc['social', 'cpt']= 0
table.loc['other', 'cpt']= 0
table.loc['total', 'cpt']= (table.loc['total', 'BDG'] - table.loc['offline', 'BDG']) / table.loc['total', 'tickets']

# OS
table.loc['adwords', 'os']= table.loc['adwords', 'orders'] / table.loc['total', 'orders']
table.loc['loyalty', 'os']= table.loc['loyalty', 'orders'] / table.loc['total', 'orders']
table.loc['crm', 'os']= table.loc['crm', 'orders'] / table.loc['total', 'orders']
table.loc['campaign', 'os']= table.loc['campaign', 'orders'] / table.loc['total', 'orders']
table.loc['affiliate', 'os']= table.loc['affiliate', 'orders'] / table.loc['total', 'orders']
table.loc['other_voucher', 'os']= table.loc['other_voucher', 'orders'] / table.loc['total', 'orders']

table.loc['display', 'os']= 0
table.loc['sms', 'os']= 0
table.loc['seo', 'os']= 0
table.loc['pr', 'os']= 0
table.loc['social', 'os']= 0
table.loc['other', 'os']= table.loc['other', 'orders'] / table.loc['total', 'orders']
table.loc['total', 'os']= table.loc['total', 'orders'] / table.loc['total', 'orders']

table.loc[:,'vertical'] = 'flight'
big_table= pd.concat([big_table, table], axis= 0)



################################################## Bus ############################################
bus = pd.read_csv(
    path + "bus.csv",
    parse_dates=["Pay At"],
    dtype={"Book ID": str},
).rename(
    columns={
        "Pay At": "date",
        "Book ID": "invoiceID",
        "Discount Code Tag": "tag",
        "Utm Tag": "utm_campaign",
        "Number Of Seats Per Book": "item",
        "Discount Code": "discount_code",
        "Discount Amount": "discount_amount",
    }
)
bus = bus[bus["Ticket Status"] != "FAILED"]
bus[(bus['date'] >= date_from) & (bus['date'] <= date_to)]

bus['discount_code'] = bus['discount_code'].str.lower()
# channel_type
index = bus[bus["discount_code"].str.startswith("slb", na=False)].index
bus.loc[index, "marketing_channel"] = "loyalty"

index = bus[bus["tag"].str.contains("journey", na=False, case=False)].index
bus.loc[index, "marketing_channel"] = "crm"

index = bus[bus["tag"].str.contains("campaign", na=False, case=False)].index
bus.loc[index, "marketing_channel"] = "campaign"

index = bus[bus["tag"].str.contains("affiliate", na=False, case=False)].index
bus.loc[index, "marketing_channel"] = "affiliate"

index = bus[(bus["marketing_channel"].isna()) & (bus["discount_code"].notna())].index
bus.loc[index, "marketing_channel"] = "other_voucher"

index = bus[bus["discount_code"].isna()].index
bus.loc[index, "marketing_channel"] = "organic"

table= pd.DataFrame(index= table_rows, columns= table_columns)

# Costs

bus_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Bus']

table.loc['adwords', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'adwords']['Cost_TT'].sum()

df= bus[bus['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'BDG']= float(df['discount_amount'].sum())

df= bus[bus['marketing_channel'] == 'crm']
table.loc['crm', 'BDG']= float(df['discount_amount'].sum())

df= bus[bus['marketing_channel'] == 'campaign']
table.loc['campaign', 'BDG']= float(df['discount_amount'].sum())

df= bus[bus['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'BDG']= (
                                float(df['discount_amount'].sum()) + 
                                bus_marketing_costs[bus_marketing_costs['channel_type'] == 'affiliate']['Cost_TT'].sum()
                                )

df= bus[bus['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'BDG']= float(df['discount_amount'].sum())
    
table.loc['display', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= bus_marketing_costs[bus_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()

table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()   ### Needs to be corrected

# Orders

table.loc['adwords', 'orders']= int(input("Bus adwords orders: "))
table.loc['loyalty', 'orders']= bus[bus['marketing_channel'] == 'loyalty']['invoiceID'].nunique()
table.loc['crm', 'orders']= bus[bus['marketing_channel'] == 'crm']['invoiceID'].nunique()
table.loc['campaign', 'orders']= bus[bus['marketing_channel'] == 'campaign']['invoiceID'].nunique()
table.loc['affiliate', 'orders']= bus[bus['marketing_channel'] == 'affiliate']['invoiceID'].nunique()
table.loc['other_voucher', 'orders']= bus[bus['marketing_channel'] == 'other_voucher']['invoiceID'].nunique()

table.loc['display', 'orders']= 0
table.loc['sms', 'orders']= 0
table.loc['seo', 'orders']= 0
table.loc['pr', 'orders']= 0
table.loc['social', 'orders']= 0
table.loc['other', 'orders']= bus[bus['marketing_channel'] == 'organic']['invoiceID'].nunique()
table.loc['total', 'orders']= bus['invoiceID'].nunique()


# Tickets

table.loc['loyalty', 'tickets']= bus[bus['marketing_channel'] == 'loyalty']['item'].sum()
table.loc['crm', 'tickets']= bus[bus['marketing_channel'] == 'crm']['item'].sum()
table.loc['campaign', 'tickets']= bus[bus['marketing_channel'] == 'campaign']['item'].sum()
table.loc['affiliate', 'tickets']= bus[bus['marketing_channel'] == 'affiliate']['item'].sum()
table.loc['other_voucher', 'tickets']= bus[bus['marketing_channel'] == 'other_voucher']['item'].sum()

table.loc['display', 'tickets']= 0
table.loc['sms', 'tickets']= 0
table.loc['seo', 'tickets']= 0
table.loc['pr', 'tickets']= 0
table.loc['social', 'tickets']= 0
table.loc['other', 'tickets']= bus[bus['marketing_channel'] == 'organic']['item'].sum()
table.loc['total', 'tickets']= bus['item'].sum()
table.loc['adwords', 'tickets']= table.loc['adwords', 'orders']*table.loc['total', 'tickets']/table.loc['total', 'orders']

# CPO
table.loc['adwords', 'cpo']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'orders']
table.loc['loyalty', 'cpo']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'orders']
table.loc['crm', 'cpo']= table.loc['crm', 'BDG'] / table.loc['crm', 'orders']
table.loc['campaign', 'cpo']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'orders']
table.loc['affiliate', 'cpo']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'orders']
try:
    table.loc['other_voucher', 'cpo']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'orders']
except:
    table.loc['other_voucher', 'cpo']= 0
table.loc['display', 'cpo']= 0
table.loc['sms', 'cpo']= 0
table.loc['seo', 'cpo']= 0
table.loc['pr', 'cpo']= 0
table.loc['social', 'cpo']= 0
table.loc['other', 'cpo']= 0
table.loc['total', 'cpo']= table.loc['total', 'BDG'] / table.loc['total', 'orders']

# CPT
table.loc['adwords', 'cpt']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'tickets']
table.loc['loyalty', 'cpt']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'tickets']
table.loc['crm', 'cpt']= table.loc['crm', 'BDG'] / table.loc['crm', 'tickets']
table.loc['campaign', 'cpt']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'tickets']
table.loc['affiliate', 'cpt']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'tickets']
table.loc['other_voucher', 'cpt']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'tickets']

table.loc['display', 'cpt']= 0
table.loc['sms', 'cpt']= 0
table.loc['seo', 'cpt']= 0
table.loc['pr', 'cpt']= 0
table.loc['social', 'cpt']= 0
table.loc['other', 'cpt']= 0
table.loc['total', 'cpt']= table.loc['total', 'BDG'] / table.loc['total', 'tickets']

# OS
table.loc['adwords', 'os']= table.loc['adwords', 'orders'] / table.loc['total', 'orders']
table.loc['loyalty', 'os']= table.loc['loyalty', 'orders'] / table.loc['total', 'orders']
table.loc['crm', 'os']= table.loc['crm', 'orders'] / table.loc['total', 'orders']
table.loc['campaign', 'os']= table.loc['campaign', 'orders'] / table.loc['total', 'orders']
table.loc['affiliate', 'os']= table.loc['affiliate', 'orders'] / table.loc['total', 'orders']
table.loc['other_voucher', 'os']= table.loc['other_voucher', 'orders'] / table.loc['total', 'orders']

table.loc['display', 'os']= 0
table.loc['sms', 'os']= 0
table.loc['seo', 'os']= 0
table.loc['pr', 'os']= 0
table.loc['social', 'os']= 0
table.loc['other', 'os']= table.loc['other', 'orders'] / table.loc['total', 'orders']
table.loc['total', 'os']= table.loc['total', 'orders'] / table.loc['total', 'orders']

table.loc[:,'vertical'] = 'bus'
big_table= pd.concat([big_table, table], axis= 0)

################################################## Int. Flight ############################################

### Note: At the time I wrote this code, Tele sales orders should
### calculate manually and add to total orders and ticket of the report


intflight = pd.read_csv(
                        path + "intflight.csv",
                        parse_dates= ['booking_date'],
                        dtype={"phone_number": str}).rename(
                            columns= {
                                'refrence_no' : 'invoiceID',
                                'discount_name' : 'discount_code',
                                'order_voucher_amount' : 'discount_amount'
                                })
intflight = intflight.dropna(subset="invoiceID")
intflight["discount_code"] = intflight["discount_code"].str.lower()
intflight[(intflight['booking_date'] >= date_from) & (intflight['booking_date'] <= date_to)]


intflight['discount_code']= intflight['discount_code'].str.lower()
# channel definitions

index = intflight[intflight["discount_code_tag"].str.contains("journey", case=False, na=False)].index
intflight.loc[index, "marketing_channel"] = "crm"

index = intflight[
    intflight["discount_code_tag"].str.contains("affiliate", case=False, na=False)
].index
intflight.loc[index, "marketing_channel"] = "affiliate"

index = intflight[intflight["discount_code_tag"].str.contains("campaign", case=False, na=False)].index
intflight.loc[index, "marketing_channel"] = "campaign"


index = intflight[intflight["discount_code"].str.startswith("slif", na=False)].index
intflight.loc[index, "marketing_channel"] = "loyalty"

index = intflight[
    (intflight["marketing_channel"].isna()) & (intflight["discount_code"].notna())
].index
intflight.loc[index, "marketing_channel"] = "other_voucher"

index = intflight[intflight["discount_code"].isna()].index
intflight.loc[index, "marketing_channel"] = "organic"


table= pd.DataFrame(index= table_rows, columns= table_columns)

# Costs

intflight_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Int. Flight']

table.loc['adwords', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'adwords']['Cost_TT'].sum()

df= intflight[intflight['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())

df= intflight[intflight['marketing_channel'] == 'crm']
table.loc['crm', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())

df= intflight[intflight['marketing_channel'] == 'campaign']
table.loc['campaign', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())

df= intflight[intflight['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'BDG']= (
                                float(df.groupby("invoiceID")["discount_amount"].unique().sum()) + 
                                intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'affiliate']['Cost_TT'].sum()
                                )
df= intflight[intflight['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())
    
table.loc['display', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= intflight_marketing_costs[intflight_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()

table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()

# Orders

table.loc['adwords', 'orders']= int(input("int. flight adwords orders: "))
table.loc['loyalty', 'orders']= intflight[intflight['marketing_channel'] == 'loyalty']['invoiceID'].nunique()
table.loc['crm', 'orders']= intflight[intflight['marketing_channel'] == 'crm']['invoiceID'].nunique()
table.loc['campaign', 'orders']= intflight[intflight['marketing_channel'] == 'campaign']['invoiceID'].nunique()
table.loc['affiliate', 'orders']= intflight[intflight['marketing_channel'] == 'affiliate']['invoiceID'].nunique()
table.loc['other_voucher', 'orders']= intflight[intflight['marketing_channel'] == 'other_voucher']['invoiceID'].nunique()

table.loc['display', 'orders']= 0
table.loc['sms', 'orders']= 0
table.loc['seo', 'orders']= 0
table.loc['pr', 'orders']= 0
table.loc['social', 'orders']= 0
table.loc['other', 'orders']= intflight[intflight['marketing_channel'] == 'organic']['invoiceID'].nunique()
table.loc['total', 'orders']= intflight['invoiceID'].nunique()


# Tickets

table.loc['loyalty', 'tickets']= intflight[intflight['marketing_channel'] == 'loyalty']['invoiceID'].count()
table.loc['crm', 'tickets']= intflight[intflight['marketing_channel'] == 'crm']['invoiceID'].count()
table.loc['campaign', 'tickets']= intflight[intflight['marketing_channel'] == 'campaign']['invoiceID'].count()
table.loc['affiliate', 'tickets']= intflight[intflight['marketing_channel'] == 'affiliate']['invoiceID'].count()
table.loc['other_voucher', 'tickets']= intflight[intflight['marketing_channel'] == 'other_voucher']['invoiceID'].count()

table.loc['display', 'tickets']= 0
table.loc['sms', 'tickets']= 0
table.loc['seo', 'tickets']= 0
table.loc['pr', 'tickets']= 0
table.loc['social', 'tickets']= 0
table.loc['other', 'tickets']= intflight[intflight['marketing_channel'] == 'organic']['invoiceID'].count()
table.loc['total', 'tickets']= intflight['invoiceID'].count()
table.loc['adwords', 'tickets']= table.loc['adwords', 'orders']*table.loc['total', 'tickets']/table.loc['total', 'orders']

# CPO
table.loc['adwords', 'cpo']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'orders']
table.loc['loyalty', 'cpo']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'orders']
table.loc['crm', 'cpo']= table.loc['crm', 'BDG'] / table.loc['crm', 'orders']
table.loc['campaign', 'cpo']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'orders']
table.loc['affiliate', 'cpo']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'orders']
table.loc['other_voucher', 'cpo']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'orders']

table.loc['display', 'cpo']= 0
table.loc['sms', 'cpo']= 0
table.loc['seo', 'cpo']= 0
table.loc['pr', 'cpo']= 0
table.loc['social', 'cpo']= 0
table.loc['other', 'cpo']= 0
table.loc['total', 'cpo']= table.loc['total', 'BDG'] / table.loc['total', 'orders']

# CPT
table.loc['adwords', 'cpt']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'tickets']
table.loc['loyalty', 'cpt']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'tickets']
table.loc['crm', 'cpt']= table.loc['crm', 'BDG'] / table.loc['crm', 'tickets']
table.loc['campaign', 'cpt']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'tickets']
table.loc['affiliate', 'cpt']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'tickets']
table.loc['other_voucher', 'cpt']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'tickets']

table.loc['display', 'cpt']= 0
table.loc['sms', 'cpt']= 0
table.loc['seo', 'cpt']= 0
table.loc['pr', 'cpt']= 0
table.loc['social', 'cpt']= 0
table.loc['other', 'cpt']= 0
table.loc['total', 'cpt']= table.loc['total', 'BDG'] / table.loc['total', 'tickets']

# OS
table.loc['adwords', 'os']= table.loc['adwords', 'orders'] / table.loc['total', 'orders']
table.loc['loyalty', 'os']= table.loc['loyalty', 'orders'] / table.loc['total', 'orders']
table.loc['crm', 'os']= table.loc['crm', 'orders'] / table.loc['total', 'orders']
table.loc['campaign', 'os']= table.loc['campaign', 'orders'] / table.loc['total', 'orders']
table.loc['affiliate', 'os']= table.loc['affiliate', 'orders'] / table.loc['total', 'orders']
table.loc['other_voucher', 'os']= table.loc['other_voucher', 'orders'] / table.loc['total', 'orders']

table.loc['display', 'os']= 0
table.loc['sms', 'os']= 0
table.loc['seo', 'os']= 0
table.loc['pr', 'os']= 0
table.loc['social', 'os']= 0
table.loc['other', 'os']= table.loc['other', 'orders'] / table.loc['total', 'orders']
table.loc['total', 'os']= table.loc['total', 'orders'] / table.loc['total', 'orders']

table.loc[:,'vertical'] = 'int_flight'
big_table= pd.concat([big_table, table], axis= 0)

################################################## Int. Hotel ############################################
inthotel = pd.read_csv(
    path + "inthotel.csv",
    parse_dates=["Registered Date"],
    dtype={"Booking ID": str, "Hotel ID": str, "Fulfilled": bool, "Mobile": str},
).rename(
    columns={
        "Registered Date": "date",
        "Booking ID": "invoiceID",
        "Tag": "tag",
        "Room Nights": "item",
        "Discount Code": "discount_code",
        "Channel": "channel",
        "Discount Value": "discount_amount",
    }
)
inthotel[(inthotel['date'] >= date_from) & (inthotel['date'] <= date_to)]
inthotel['discount_code']= inthotel['discount_code'].str.lower()
# channel_type
index = inthotel[
    (inthotel["discount_code"].str.startswith("slih", na=False))
    &
    (inthotel['discount_amount'] != 0)
    ].index
inthotel.loc[index, "marketing_channel"] = "loyalty"

index = inthotel[
    (inthotel["tag"].str.contains("journey", na=False, case=False))
    &
    (inthotel['discount_amount'] != 0)
    ].index
inthotel.loc[index, "marketing_channel"] = "crm"

index = inthotel[
    (inthotel["tag"].str.contains("campaign", na=False, case=False))
    &
    (inthotel['discount_amount'] != 0)
    ].index
inthotel.loc[index, "marketing_channel"] = "campaign"

index = inthotel[
    (inthotel["tag"].str.contains("affiliate", na=False, case=False))
    &
    (inthotel['discount_amount'] != 0)
    ].index
inthotel.loc[index, "marketing_channel"] = "affiliate"

index = inthotel[
    (inthotel["marketing_channel"].isna())
    & 
    (inthotel["discount_code"].notna())
    &
    (inthotel['discount_amount'] != 0)
    ].index
inthotel.loc[index, "marketing_channel"] = "other_voucher"

index = inthotel[inthotel["marketing_channel"].isna()].index
inthotel.loc[index, "marketing_channel"] = "organic"

table= pd.DataFrame(index= table_rows, columns= table_columns)

# Costs

inthotel_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Int. Hotel']

table.loc['adwords', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'adwords']['Cost_TT'].sum()

df= inthotel[inthotel['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'BDG']= float(df['discount_amount'].sum())

df= inthotel[inthotel['marketing_channel'] == 'crm']
table.loc['crm', 'BDG']= float(df['discount_amount'].sum())

df= inthotel[inthotel['marketing_channel'] == 'campaign']
table.loc['campaign', 'BDG']= float(df['discount_amount'].sum())

df= inthotel[inthotel['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'BDG']= (
                                float(df['discount_amount'].sum()) + 
                                inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'affiliate']['Cost_TT'].sum()
                                )

df= inthotel[inthotel['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'BDG']= float(df['discount_amount'].sum())
    
table.loc['display', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= inthotel_marketing_costs[inthotel_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()

table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()   ### Needs to be corrected

# Orders

table.loc['adwords', 'orders']= int(input("Int. Hotel adwords orders: "))
table.loc['loyalty', 'orders']= inthotel[inthotel['marketing_channel'] == 'loyalty']['invoiceID'].nunique()
table.loc['crm', 'orders']= inthotel[inthotel['marketing_channel'] == 'crm']['invoiceID'].nunique()
table.loc['campaign', 'orders']= inthotel[inthotel['marketing_channel'] == 'campaign']['invoiceID'].nunique()
table.loc['affiliate', 'orders']= inthotel[inthotel['marketing_channel'] == 'affiliate']['invoiceID'].nunique()
table.loc['other_voucher', 'orders']= inthotel[inthotel['marketing_channel'] == 'other_voucher']['invoiceID'].nunique()

table.loc['display', 'orders']= 0
table.loc['sms', 'orders']= 0
table.loc['seo', 'orders']= 0
table.loc['pr', 'orders']= 0
table.loc['social', 'orders']= 0
table.loc['other', 'orders']= inthotel[inthotel['marketing_channel'] == 'organic']['invoiceID'].nunique()
table.loc['total', 'orders']= inthotel['invoiceID'].nunique()


# Room Nights

table.loc['loyalty', 'tickets']= inthotel[inthotel['marketing_channel'] == 'loyalty']['item'].sum()
table.loc['crm', 'tickets']= inthotel[inthotel['marketing_channel'] == 'crm']['item'].sum()
table.loc['campaign', 'tickets']= inthotel[inthotel['marketing_channel'] == 'campaign']['item'].sum()
table.loc['affiliate', 'tickets']= inthotel[inthotel['marketing_channel'] == 'affiliate']['item'].sum()
table.loc['other_voucher', 'tickets']= inthotel[inthotel['marketing_channel'] == 'other_voucher']['item'].sum()

table.loc['display', 'tickets']= 0
table.loc['sms', 'tickets']= 0
table.loc['seo', 'tickets']= 0
table.loc['pr', 'tickets']= 0
table.loc['social', 'tickets']= 0
table.loc['other', 'tickets']= inthotel[inthotel['marketing_channel'] == 'organic']['item'].sum()
table.loc['total', 'tickets']= inthotel['item'].sum()
table.loc['adwords', 'tickets']= table.loc['adwords', 'orders']*table.loc['total', 'tickets']/table.loc['total', 'orders']

# CPO
try:
    table.loc['adwords', 'cpo']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'orders']
except:
    table.loc['adwords', 'cpo']= 0
try:
    table.loc['loyalty', 'cpo']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'orders']
except:
    table.loc['loyalty', 'cpo']= 0
try:
    table.loc['crm', 'cpo']= table.loc['crm', 'BDG'] / table.loc['crm', 'orders']
except:
    table.loc['crm', 'cpo']= 0
try:
    table.loc['campaign', 'cpo']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'orders']
except:
    table.loc['campaign', 'cpo']= 0
try:
    table.loc['affiliate', 'cpo']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'orders']
except:
    table.loc['affiliate', 'cpo']= 0
try:
    table.loc['other_voucher', 'cpo']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'orders']
except:
    table.loc['other_voucher', 'cpo']= 0
table.loc['display', 'cpo']= 0
table.loc['sms', 'cpo']= 0
table.loc['seo', 'cpo']= 0
table.loc['pr', 'cpo']= 0
table.loc['social', 'cpo']= 0
table.loc['other', 'cpo']= 0
table.loc['total', 'cpo']= table.loc['total', 'BDG'] / table.loc['total', 'orders']

# CPT
try:
    table.loc['adwords', 'cpt']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'tickets']
except:
    table.loc['adwords', 'cpt']= 0
try:
    table.loc['loyalty', 'cpt']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'tickets']
except:
    table.loc['loyalty', 'cpt']= 0
try:
    table.loc['crm', 'cpt']= table.loc['crm', 'BDG'] / table.loc['crm', 'tickets']
except:
    table.loc['crm', 'cpt']= 0
try:
    table.loc['campaign', 'cpt']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'tickets']
except:
    table.loc['campaign', 'cpt']= 0
try:
    table.loc['affiliate', 'cpt']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'tickets']
except:
    table.loc['affiliate', 'cpt']= 0
try:
    table.loc['other_voucher', 'cpt']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'tickets']
except:
    table.loc['other_voucher', 'cpt']= 0
table.loc['display', 'cpt']= 0
table.loc['sms', 'cpt']= 0
table.loc['seo', 'cpt']= 0
table.loc['pr', 'cpt']= 0
table.loc['social', 'cpt']= 0
table.loc['other', 'cpt']= 0
table.loc['total', 'cpt']= table.loc['total', 'BDG'] / table.loc['total', 'tickets']

# OS
table.loc['adwords', 'os']= table.loc['adwords', 'orders'] / table.loc['total', 'orders']
table.loc['loyalty', 'os']= table.loc['loyalty', 'orders'] / table.loc['total', 'orders']
table.loc['crm', 'os']= table.loc['crm', 'orders'] / table.loc['total', 'orders']
table.loc['campaign', 'os']= table.loc['campaign', 'orders'] / table.loc['total', 'orders']
table.loc['affiliate', 'os']= table.loc['affiliate', 'orders'] / table.loc['total', 'orders']
table.loc['other_voucher', 'os']= table.loc['other_voucher', 'orders'] / table.loc['total', 'orders']

table.loc['display', 'os']= 0
table.loc['sms', 'os']= 0
table.loc['seo', 'os']= 0
table.loc['pr', 'os']= 0
table.loc['social', 'os']= 0
table.loc['other', 'os']= table.loc['other', 'orders'] / table.loc['total', 'orders']
table.loc['total', 'os']= table.loc['total', 'orders'] / table.loc['total', 'orders']

table.loc[:,'vertical'] = 'Inthotel'
big_table= pd.concat([big_table, table], axis= 0)
################################################## Train ############################################
train = pd.read_csv(
    path + "train.csv",
    dtype={ "date" : str,
            "invoiceID": str,
           "discount_code": str,
           "Mobile Number": str},
).rename(
    columns= {
        'Order ID' : 'invoiceID',
        'Discount Code' : 'discount_code',
        'Paid At' : 'date',
        'Discount Price' : 'discount_amount'
        }
    )
train["date"] = train["date"].str[:10]
train["Mobile Number"] = train["Mobile Number"].str[-10:]
train = train.sort_values(by="date")
train= train[(train['date'] >= date_from) & (train['date'] <= date_to)]
train["discount_amount"].fillna(0, inplace=True)

# tagging

sh = client.open("Marketing Discount Codes and Categories")
worksheet = sh.worksheet("Train")

tags_sheet = worksheet.get_values()
tags_sheet = pd.DataFrame(tags_sheet)
headers = tags_sheet.iloc[0]
tags_sheet.columns = headers
tags_sheet = tags_sheet[1:]
tags_sheet.reset_index(drop=True, inplace=True)
tags_sheet.reset_index(drop=True, inplace=True)

tags_sheet= tags_sheet.rename(
    columns={"discountcategory ": "discount_category"}
)
tags_sheet["discountcode"] = tags_sheet["discountcode"].str.lower()
train["discount_code"] = train["discount_code"].str.lower()

for discount_code in train["discount_code"].unique():
    index = train[train["discount_code"] == discount_code].index
    try:
        tag = tags_sheet[tags_sheet["discountcode"].str[:5] == discount_code[:5]][
            "discount_category"
        ].values[-1]
        train.loc[index, "tag"] = tag
    except:
        continue


table= pd.DataFrame(index= table_rows, columns= table_columns)


index = train[train["tag"].str.contains("journey", case=False, na=False)].index
train.loc[index, "marketing_channel"] = "crm"

index = train[train["tag"].str.contains("affiliate", case=False, na=False)].index
train.loc[index, "marketing_channel"] = "affiliate"

index = train[train["tag"].str.contains("campaign", case=False, na=False)].index
train.loc[index, "marketing_channel"] = "campaign"

index = train[train["tag"].str.contains("loyalty", case=False, na=False)].index
train.loc[index, "marketing_channel"] = "loyalty"

index = train[(train["marketing_channel"].isna()) & (train["discount_code"].notna())].index
train.loc[index, "marketing_channel"] = "other_voucher"

index = train[train["discount_code"].isna()].index
train.loc[index, "marketing_channel"] = "organic"

table_rows= ['adwords', 'loyalty', 'crm', 'campaign', 'affiliate', 'other_voucher', 'display', 'sms', 'seo', 'pr', 'social', 'other', 'total']
table_columns= ['BDG', 'orders', 'tickets', 'cpo', 'cpt', 'os']

table= pd.DataFrame(index= table_rows, columns= table_columns)

# Costs

train_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Train']

table.loc['adwords', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'adwords']['Cost_TT'].sum()

df= train[train['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())

df= train[train['marketing_channel'] == 'crm']
table.loc['crm', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())

df= train[train['marketing_channel'] == 'campaign']
table.loc['campaign', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())

df= train[train['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'BDG']= (
                                float(df.groupby("invoiceID")["discount_amount"].unique().sum()) + 
                                train_marketing_costs[train_marketing_costs['channel_type'] == 'affiliate']['Cost_TT'].sum()
                                )

df= train[train['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'BDG']= float(df.groupby("invoiceID")["discount_amount"].unique().sum())
   
table.loc['display', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= train_marketing_costs[train_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()

table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()

# Orders

table.loc['adwords', 'orders']= int(input("Train adwords orders: "))
table.loc['loyalty', 'orders']= train[train['marketing_channel'] == 'loyalty']['invoiceID'].nunique()
table.loc['crm', 'orders']= train[train['marketing_channel'] == 'crm']['invoiceID'].nunique()
table.loc['campaign', 'orders']= train[train['marketing_channel'] == 'campaign']['invoiceID'].nunique()
table.loc['affiliate', 'orders']= train[train['marketing_channel'] == 'affiliate']['invoiceID'].nunique()
table.loc['other_voucher', 'orders']= train[train['marketing_channel'] == 'other_voucher']['invoiceID'].nunique()

table.loc['display', 'orders']= 0
table.loc['sms', 'orders']= 0
table.loc['seo', 'orders']= 0
table.loc['pr', 'orders']= 0
table.loc['social', 'orders']= 0
table.loc['other', 'orders']= train[train['marketing_channel'] == 'organic']['invoiceID'].nunique()
table.loc['total', 'orders']= train['invoiceID'].nunique()


# Tickets

df= train[train['marketing_channel'] == 'loyalty']
table.loc['loyalty', 'tickets']= (df['Empty Seat Fraction Per Route'].sum() + len(df))

df= train[train['marketing_channel'] == 'crm']
table.loc['crm', 'tickets']= (df['Empty Seat Fraction Per Route'].sum() + len(df))

df= train[train['marketing_channel'] == 'campaign']
table.loc['campaign', 'tickets']= (df['Empty Seat Fraction Per Route'].sum() + len(df))

df= train[train['marketing_channel'] == 'affiliate']
table.loc['affiliate', 'tickets']= (df['Empty Seat Fraction Per Route'].sum() + len(df))

df= train[train['marketing_channel'] == 'other_voucher']
table.loc['other_voucher', 'tickets']= (df['Empty Seat Fraction Per Route'].sum() + len(df))

table.loc['display', 'tickets']= 0
table.loc['sms', 'tickets']= 0
table.loc['seo', 'tickets']= 0
table.loc['pr', 'tickets']= 0
table.loc['social', 'tickets']= 0

df= train[train['marketing_channel'] == 'organic']
table.loc['other', 'tickets']= (df['Empty Seat Fraction Per Route'].sum() + len(df))

table.loc['total', 'tickets']= (train['Empty Seat Fraction Per Route'].sum() + len(train))
table.loc['adwords', 'tickets']= table.loc['adwords', 'orders']*table.loc['total', 'tickets']/table.loc['total', 'orders']

try:
    # CPO
    table.loc['adwords', 'cpo']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'orders']
except:
    table.loc['adwords', 'cpo']= 0
try:
    table.loc['loyalty', 'cpo']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'orders']
except:
    table.loc['loyalty', 'cpo']= 0
try:
    table.loc['crm', 'cpo']= table.loc['crm', 'BDG'] / table.loc['crm', 'orders']
except:
    table.loc['crm', 'cpo']= 0
try:
    table.loc['campaign', 'cpo']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'orders']
except:
    table.loc['campaign', 'cpo']= 0
try:
    table.loc['affiliate', 'cpo']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'orders']
except:
    table.loc['affiliate', 'cpo']= 0
try:
    table.loc['other_voucher', 'cpo']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'orders']
except:
    table.loc['other_voucher', 'cpo']= 0
    
table.loc['display', 'cpo']= 0
table.loc['sms', 'cpo']= 0
table.loc['seo', 'cpo']= 0
table.loc['pr', 'cpo']= 0
table.loc['social', 'cpo']= 0
table.loc['other', 'cpo']= 0
table.loc['total', 'cpo']= table.loc['total', 'BDG'] / table.loc['total', 'orders']
    
# CPT
try:
    table.loc['adwords', 'cpt']= table.loc['adwords', 'BDG'] / table.loc['adwords', 'tickets']
except:
    table.loc['adwords', 'cpt']= 0
try:
    table.loc['loyalty', 'cpt']= table.loc['loyalty', 'BDG'] / table.loc['loyalty', 'tickets']
except:
    table.loc['loyalty', 'cpt']= 0
try:
    table.loc['crm', 'cpt']= table.loc['crm', 'BDG'] / table.loc['crm', 'tickets']
except:
    table.loc['crm', 'cpt']= 0
try:
    table.loc['campaign', 'cpt']= table.loc['campaign', 'BDG'] / table.loc['campaign', 'tickets']
except:
    table.loc['campaign', 'cpt']= 0
try:
    table.loc['affiliate', 'cpt']= table.loc['affiliate', 'BDG'] / table.loc['affiliate', 'tickets']
except:
    table.loc['affiliate', 'cpt']= 0
try:
    table.loc['other_voucher', 'cpt']= table.loc['other_voucher', 'BDG'] / table.loc['other_voucher', 'tickets']
except:
    table.loc['other_voucher', 'cpt']= 0

table.loc['display', 'cpt']= 0
table.loc['sms', 'cpt']= 0
table.loc['seo', 'cpt']= 0
table.loc['pr', 'cpt']= 0
table.loc['social', 'cpt']= 0
table.loc['other', 'cpt']= 0
table.loc['total', 'cpt']= table.loc['total', 'BDG'] / table.loc['total', 'tickets']

# OS
table.loc['adwords', 'os']= table.loc['adwords', 'orders'] / table.loc['total', 'orders']
table.loc['loyalty', 'os']= table.loc['loyalty', 'orders'] / table.loc['total', 'orders']
table.loc['crm', 'os']= table.loc['crm', 'orders'] / table.loc['total', 'orders']
table.loc['campaign', 'os']= table.loc['campaign', 'orders'] / table.loc['total', 'orders']
table.loc['affiliate', 'os']= table.loc['affiliate', 'orders'] / table.loc['total', 'orders']
table.loc['other_voucher', 'os']= table.loc['other_voucher', 'orders'] / table.loc['total', 'orders']
table.loc['display', 'os']= 0
table.loc['sms', 'os']= 0
table.loc['seo', 'os']= 0
table.loc['pr', 'os']= 0
table.loc['social', 'os']= 0
table.loc['other', 'os']= table.loc['other', 'orders'] / table.loc['total', 'orders']
table.loc['total', 'os']= table.loc['total', 'orders'] / table.loc['total', 'orders']
table.fillna(0, inplace= True)

table.loc[:,'vertical'] = 'train'
big_table= pd.concat([big_table, table], axis= 0)

########################################## Brand ######################################################

brand_marketing_costs= marketing_spending[marketing_spending['Product'] == 'Brand']

table= pd.DataFrame(index= table_rows, columns= table_columns)

table.loc['display', 'BDG']= brand_marketing_costs[brand_marketing_costs['channel_type'] == 'display']['Cost_TT'].sum()

table.loc['sms', 'BDG']= brand_marketing_costs[brand_marketing_costs['channel_type'] == 'sms']['Cost_TT'].sum()

table.loc['seo', 'BDG']= brand_marketing_costs[brand_marketing_costs['channel_type'] == 'seo']['Cost_TT'].sum()

table.loc['pr', 'BDG']= brand_marketing_costs[brand_marketing_costs['channel_type'] == 'pr']['Cost_TT'].sum()

table.loc['social', 'BDG']= brand_marketing_costs[brand_marketing_costs['channel_type'] == 'social']['Cost_TT'].sum()

table.loc['offline', 'BDG']= brand_marketing_costs[brand_marketing_costs['channel_type'] == 'offline']['Cost_TT'].sum()

table.loc['other', 'BDG']= 0

table.loc['total', 'BDG']= table.loc[:, 'BDG'].sum()

table.loc[:,'vertical'] = 'brand'
big_table= pd.concat([big_table, table], axis= 0)

big_table.to_csv(path + "big_table.csv")

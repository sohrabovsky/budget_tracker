# marketing_channel_performance_report
This code is wrote for calculating different marketing channels' sales and spending for calculation of CPO and performance of overall 
marketing activities as well as each channel separately.
The final report is included with three tables:
* Plan
* Actual
* Forecast

In this code, i'm going to calculate performance, spending, CPO, and CPT/CPRN (cost per ticket/cost per room night) for 
marketing channels of ticket selling and hotel selling verticals.

For this purpose, I need various resources, because data is imported from different tables for each verticals. Also, we need to 
import a marketing spending google sheet which is updating by human in marketing department.

The calculation of this code then should be imported in an Excel file which I imported here with the name: template.xlsx

In this excel file, Plan table should be inserted as what is marketing plan. Actual and Forecast tables are filled with my script.
I imported two different scripts here, one for calculation of Actual tables and the other for calculation of Forecasting. 

Please note that forecasting each marketing channel separately is not something that I generally do with code. I just forecast overall sales
at the end of the month and after that, I will breakdown this overall sales to each marketing channels according to people insights and each
marketing channel share from total sales.

The output of the actuals.py is stored in a .csv file named big_table.csv which is actual report.
The output of the forecasting.py is stored in a .csv file named table.csv which is forecast report.

Some definitions for this report:

Adwords: 
* Type: Performance Channel
* Costs Source: Marketing Spending Sheet (Google Sheet)
* Source Link: https://docs.google.com/spreadsheets/d/1zxPndjoFHYjo0Mbt-BBtSJgwWwDn7q7u45uIb0O96fs/edit#gid=375972121
* Subcategories: None
* Order Source: Imported from Adwords channel by asking ads operators:
    * Hedie Razi - Dom. Hotel, Dom. Flight, Int. Flight
    * Hanieh - Bus, Train



Vouchers:
* Type: Performance Channel
* Source Name: Metabase
* Source Link:
    * Dom. Hotel: Marketing Hotel Prefix With Utm
        * Filters: fulfilled is 1
    * Dom. Flight: Flight UTM & Discount Data Per Order
        * Filters: None
    * Bus: Bus Book Ticket New
        * Filters: None
    * Int-Flight: Flight International Marketing Discount Prefix and Tag- Distinct
        * Filters: None
    * Int-Hotel: Marketing Int Hotel Prefixes
        * Filters: Fulfilled is 1
    * Train: Train Tickets
        * Filters: None
* Subcategories:
    * Loyalty:
        * Channel Definition: Snapp loyalty club (کلاب اسنپ)
    * CRM:
        * Channel Definition: Journeys of webengage
    * Campaign:
        * Channel Definition: Jek, Snapptrip, and Social campaigns
    * Affiliate:
        * Channel Definition: vouchers that are allocated to affiliators such as: Takhfifan, Mopon, etc.
    * Other Voucher: 
        * Channel Definition: vouchers that are rare and are not in other ordinary voucher categories
    * Cross Sell: 
        * Channel Definition: Journeys of webengage that are related to cross selling. Just measured in dom. Hotel

PR/Backlink:
* Type: Non-Performance Channel
* Cost Source: Marketing Spending Sheet (owner: Bahar)
* Channel Definition: backlinks and reportage articles to enhance SEO

SMS:
* Type: Non-Performance Channel
* Cost Source: Marketing Spending Sheet (owners: Omid and Hanieh)
* Channel Definition: SMS costs for campaigns and webengage journeys
    
Social:
* Type: Non-Performance Channel
* Cost Source: Marketing Spending Sheet (owners: Khatereh, Asra)
* Channel Definition: Cost of social campaigns and tools

Content (SEO):
* Type: Non-Performance Channel
* Cost Source: Marketing Spending Sheet (owners: Bahar)
* Channel Definition: Cost of social campaigns and tools

Offline:
* Type: Non-Performance Channel
* Cost Source: Marketing Spending Sheet (owners: Omid)
* Channel Definition: Cost of offline (not online) activities

Deduplicate:
This is a line in report for adjustment of orders duplicated by various channels( Adwords vs voucher)


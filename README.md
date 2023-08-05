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

The output of the code.py is stored in a .csv file named big_table.csv which is actual report.

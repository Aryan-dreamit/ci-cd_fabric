# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Main Code

# CELL ********************

import pandas as pd
import numpy as np
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1-2-2025
date = datetime.datetime.strptime("03-01-2025", '%m-%d-%Y').date() # old_names[i].strip()
formatted_date = f"{date.day}-{date.month}-{date.year}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_file_paths = {
    "CVS": "./builtin/Power BI template Update - CVS 03-01-25.xlsx",
    "Kroger": {
        "data": "./builtin/BDF Kroger Weekly Sales 2025 P1W4.xlsx",
        "calender": "./builtin/calender/Calender kroger.xlsx"
    },
    "Walmart": {
        "data": "./builtin/New Weekly Sales - Prachi Update - Only TY.xlsx",
        "calender": "./builtin/calender/Walmart Weeks.xlsx"
    },
    "Amazon": "./builtin/1. Weekly POS Data Dump (9).xlsx",
    "TargetOnline": "./builtin/BDF Power BI Pull 2 Wk 4.xlsx",
    "TargetStore": "./builtin/BDF Power BI Pull 2 Wk 4.xlsx",
    "RiteAid": "./builtin/RiteAid WE 3-1-2025.xlsx",
    "Walgreens": "./builtin/Item Level Weekly $ and Units (8).xlsx"
}

sources = ['Excel', 'SQL', 'PBI','Missing_UPC']
retailers = raw_file_paths.keys()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SQL
# target db is taking too much time

# CELL ********************

import pyodbc

def get_data_from_sql(dates, retailers_list):
    # SQL Connection
    SERVER = 'dreamit.database.windows.net'
    DATABASE = 'BDF'
    USERNAME = 'Dreamit'
    PASSWORD = 'Cognizant@12356'
    connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    conn = pyodbc.connect(connectionString)
    cursor = conn.cursor()

    # Database Definations
    retailers = {
        "Amazon": "AmazonwithASINCode", 
        "CVS": "CVS", 
        "RiteAid": "RiteAid_new", 
        "Target": "Target_with_Store", # takes too much time, just too much, find the issue 
        "Walgreens": "Walgreens_new ", 
        "Walmart": "WalmartReload_new", 
        "Kroger": "Kroger_new"
        }
                                                                        
    return_data = {}

    # Getting the data
    for date in dates:
        query_date = f"{date.month}-{date.day}-{date.year}"
        store_date = f"{date.day}-{date.month}-{date.year}"
        vals=[]
        for retailer in retailers_list:
            if retailer in retailers.keys():
                if retailer == "Walmart": # it has 'Value" col, rather then 'Values'
                    SQL_QUERY = f"""SELECT [Date], SUM([Value]) FROM [dbo].[{retailers[retailer]}] WHERE Date = '{query_date}' GROUP BY [Date]"""
                else:
                    SQL_QUERY = f"""SELECT [Date], SUM([Values]) FROM [dbo].[{retailers[retailer]}] WHERE Date = '{query_date}' GROUP BY [Date]""" #  group by [Date] order by [Date]
                cursor.execute(SQL_QUERY)
                records = cursor.fetchall()
                vals.append((retailer, float(records[0][1])))
        return_data[store_date] = vals

    return return_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_data_from_sql([date], ['Kroger'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Power BI

# CELL ********************

import sempy.fabric as fabric

def get_data_from_semantic_model(dates, retailers):
    retailers = [
        "Amazon",
        "CVS",
        "Kroger",
        "RiteAid",
        "Target",
        "Walgreens",
        "Walmart"
    ]

    # FORMING DATE STRING FOR DAX QUERY
    dates_string = [f"DATE({date.year},{date.month},{date.day})" for date in dates]
    dates_string = '{'+','.join(dates_string)+'}'

    return_data = {}

    # DATA OF EVERYONE OTHER THEN TARGET ONLINE
    for retailer in retailers:
        dax_query=f"""
            EVALUATE(
                FILTER(
                    FactTable,FactTable[Date] 
                        in {dates_string}
                    && FactTable[Customer]="{retailer}")
            )"""
        data = fabric.evaluate_dax(dataset="Weekly POS PBI 2025", dax_string=dax_query)
        data = data.groupby("FactTable[Date]")['FactTable[Value]'].sum()

        for i in data.index:
            date=f"{i.day}-{i.month}-{i.year}"
            if date in return_data.keys():
                if retailer == "Target":
                    return_data[date].append(("TargetStore", round(data[i],2)))
                else:
                    return_data[date].append((retailer, round(data[i],2)))
            else:
                return_data[date] = [(retailer, round(data[i],2))]

    # TARGET ONLINE QUERY
    dax_query = f"""
        EVALUATE(
            FILTER(
                SELECTCOLUMNS(
                    'Target_dot_com_data_2023', 
                    "Date", [Date], 
                    "Value", [Values]
                    ),
                [Date] IN {dates_string}
                )
            )
        """
    data = fabric.evaluate_dax(dataset="Weekly POS PBI 2025", dax_string=dax_query)
    data = data.groupby("[Date]")['[Value]'].sum()
    for i in data.index:
        date=f"{i.day}-{i.month}-{i.year}"
        if date in return_data.keys():
            return_data[date].append(("TargetOnline", round(data[i],2)))
        else:
            return_data[date] = [("TargetOnline", round(data[i],2))]
    return return_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_data_from_semantic_model([date],['Kroger'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Raw Files

# MARKDOWN ********************

# ### CVS

# CELL ********************

class CVS:
    def __init__(self, file_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown File Format")

        # PREP DATA
        data.dropna(how='all', inplace=True)
        data.columns = data.iloc[0].values
        data.drop(index=data.index[0], axis=0, inplace=True)
        
        # rename columns
        new_names = ['BDF Brand Category', 'BDF Brand Pillar', 'Brand', 'Product', 'SKU', 'SKU Desc', 'SKU Size', 'UPC', 'Category Desc']
        old_names = data.columns
        date_cols = []
        for i in range(len(new_names)):
            data.rename(columns={old_names[i]: new_names[i]}, inplace=True)
        
        # remove spaces from dates and cast them
        for i in range(len(new_names), len(old_names)):
            a = datetime.datetime.strptime(old_names[i].strip(), '%m-%d-%Y').date()
            data.rename(columns={old_names[i]: a}, inplace=True)
            date_cols.append(a)
        
        # OUTPUT DATES
        self.data = data
        self.dates = date_cols
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{date.day}-{date.month}-{date.year}' for date in self.dates]}")
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        sales_sum = self.data.loc[:,date].sum()
        return_date = f"{date.day}-{date.month}-{date.year}"
        return {return_date: ("CVS", round(sales_sum,2))}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cvs = CVS(r"./builtin/Power BI template Update - CVS 03-01-25.xlsx")
cvs.get_data()
cvs.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### RiteAid

# CELL ********************

class RiteAid:
    def __init__(self, file_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path, sheet_name=1)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown File Format")

        # PREP DATA
        data.drop(data.tail(8).index,inplace=True)
        data = data.rename(columns={'W/E': 'Date'}).fillna(0)
        data['Date'] = pd.to_datetime(data['Date'])
        
        # OUTPUT DATES
        self.data = data
        self.dates = data['Date'].unique()
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{date.day}-{date.month}-{date.year}' for date in self.dates]}")
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        return_date = f"{date.day}-{date.month}-{date.year}"
        sales_sum = self.data.query(f"Date=='{date}'")['Sales ($) (total)'].sum().round(2)
 
        return {return_date: ("RiteAid", round(sales_sum,2))}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.read_excel((raw_file_paths["RiteAid"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

riteaid = RiteAid(raw_file_paths["RiteAid"])
# riteaid.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# riteaid.get_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# riteaid.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Target Store

# CELL ********************

import pandas as pd


class TargetStore:
    def __init__(self, file_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path, sheet_name=0)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown File Format")

        # PREP DATA
        data.drop(data.tail(8).index,inplace=True)
        data['Date'] = pd.to_datetime(data['Date'])
        data['Date'] = data["Date"] + datetime.timedelta(days=6)
        
        # OUTPUT DATES
        self.data = data
        self.dates = data['Date'].unique()
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{date.day}-{date.month}-{date.year}' for date in self.dates]}")
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        return_date = f"{date.day}-{date.month}-{date.year}"
        sales_sum = self.data.query(f"Date=='{date}'")['Sales $'].sum().round(2)
        # return {return_date: sales_sum}
        # sales_sum = self.data.loc[:,date].sum()
        return {return_date: ("TargetStore", round(sales_sum,2))}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# targetstore = TargetStore(r"./builtin/BDF Power BI Pull 1 Wk 4.xlsx")
# targetstore.get_data()
# targetstore.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Target Online

# CELL ********************

class TargetOnline:
    def __init__(self, file_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path, sheet_name=1)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown File Format")

        # PREP DATA
        data.drop(data.tail(12).index,inplace=True)
        data['DATE'] = pd.to_datetime(data['DATE'])
        data['DATE'] = data["DATE"] + datetime.timedelta(days=6)
        
        # OUTPUT DATES
        self.data = data
        self.dates = data['DATE'].unique()
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{date.day}-{date.month}-{date.year}' for date in self.dates]}")
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        return_date = f"{date.day}-{date.month}-{date.year}"
        sales_sum = self.data.query(f"DATE=='{date}'")['SALES $'].sum().round(2)
        return {return_date: ("TargetOnline", round(sales_sum,2))}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# targetonline = TargetOnline(r"./builtin/BDF Power BI Pull 1 Wk 4.xlsx")
# targetonline.get_data()
# targetonline.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Amazon

# CELL ********************

class Amazon:
    def __init__(self, file_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path, sheet_name=0)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown File Format")

        # PREP DATA
        data.dropna(how='all', inplace=True)
        data['Reporting Period'] = pd.to_datetime(data['Reporting Period'])
        
        # OUTPUT DATES
        self.data = data
        self.dates = data['Reporting Period'].unique()
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{date.day}-{date.month}-{date.year}' for date in self.dates]}")
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        return_date = f"{date.day}-{date.month}-{date.year}"
        sales_sum = self.data.query(f"`Reporting Period`=='{date}'")['Ordered Revenue'].sum().round(2)
        return {return_date: ("Amazon", round(sales_sum,2))}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# amazon = Amazon(r"./builtin/1. Weekly POS Data Dump (5).xlsx")
# amazon.get_data()
# amazon.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Kroger

# CELL ********************

class Kroger:
    def __init__(self, file_path, calender_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        self.calender_path = calender_path

        # data file
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

        # calender file
        if calender_path[-4:] == 'xlsx':
            self.calender_file_format = 'excel'
        elif calender_path[-3:] == 'csv':
            self.calender_file_format = 'csv'
        else:
            self.calender_file_format = 'unknown'


    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD DATA FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path, sheet_name="DATA")
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown Data File Format")

        # LOAD CALENDER FILE
        if self.calender_file_format == 'excel':
            calender = pd.read_excel(self.calender_path, sheet_name=0)
        if self.calender_file_format == 'csv':
            calender = pd.read_csv(self.calender_path)
        if self.calender_file_format == 'unknown':
            raise Exception("Unknown Calender File Format")

        # PREP DATA
        date_string_raw = data.iloc[2, 13].replace(" ", "").split("From:")[1].split("to")[0]
        week = int(date_string_raw.split("WK")[1].split("(")[1].replace(")", ""))
        year = int(date_string_raw[:4])
        date = calender.query(f"FiscalYear == {year} and Week == {week}")['WeekEndDate'].values[0]

        brands_list = [
            'NIVEA BODYWASH MENS',
            'NIVEA BODYWASH WOMENS',
            'AQUAPHOR BABY',
            'EUCERIN BABY',
            'EUCERIN FACE',
            'EUCERIN HBL',
            'AQUAPHOR HBL',
            'EUCERIN SUN',
            'NIVEA HBL',
            'NIVEA FOR MEN',
            'AQUAPHOR LIP',
            'COPPERTONE'
        ]
        data = data.drop(data.head(41).index).reset_index().drop(columns=['index'])
        data.columns = data.iloc[0].values
        data.drop(0, inplace=True)
        data.drop(columns=['Column1'], inplace=True)
        data = data.query(f"SCANNED_RETAIL_DOLLARS_CUR.notna() and TIMEFRAME=='01W' and BRAND in {brands_list}")


        # data.dropna(how='all', inplace=True)
        # data['Reporting Period'] = pd.to_datetime(data['Reporting Period'])
        
        # OUTPUT DATES
        self.data = data
        self.dates = [date]
        print(f'''Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{(date - date.astype("datetime64[M]") + 1).astype(int)}-{date.astype("datetime64[M]").astype(int) % 12 + 1}-{date.astype("datetime64[Y]").astype(int) + 1970}' for date in self.dates]}''')
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        # np.datetime64 to datetime.datetime to datetime.date
        dt64 = self.dates[0]
        unix_epoch = np.datetime64(0, "s")
        one_second = np.timedelta64(1, "s")
        seconds_since_epoch = (dt64 - unix_epoch) / one_second

        new_date = datetime.datetime.utcfromtimestamp(seconds_since_epoch).date()
        if date == new_date:
            return_date = f"{date.day}-{date.month}-{date.year}"
            sales_sum = self.data['SCANNED_RETAIL_DOLLARS_CUR'].sum()
            # sales_sum = self.data.query(f"`Reporting Period`=='{date}'")['Ordered Revenue'].sum().round(2)
            return {return_date: ("Kroger", round(sales_sum,2))}
        else:
            raise Exception("Date not found in data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# kroger = Kroger(r"./builtin/BDF Kroger Weekly Sales 2024 P13W4 02-01-2025.xlsx", r"./builtin/calender/Calender kroger.xlsx")
# kroger.get_data()
# kroger.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Walmart

# CELL ********************

class Walmart:
    def __init__(self, file_path, calender_path):
        '''prep the varibles and general information'''
        self.file_path = file_path
        self.calender_path = calender_path

        # data file
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

        # calender file
        if calender_path[-4:] == 'xlsx':
            self.calender_file_format = 'excel'
        elif calender_path[-3:] == 'csv':
            self.calender_file_format = 'csv'
        else:
            self.calender_file_format = 'unknown'


    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''

        # LOAD DATA FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown Data File Format")

        # LOAD CALENDER FILE
        if self.calender_file_format == 'excel':
            calender = pd.read_excel(self.calender_path)
        if self.calender_file_format == 'csv':
            calender = pd.read_csv(self.calender_path)
        if self.calender_file_format == 'unknown':
            raise Exception("Unknown Calender File Format")

        # PREP DATA
        calender = calender.dropna(how="all")
        drop_cols = [col for col in data.columns if "last_year" in col or "item_quantity" in col]
        data = data.drop(columns=drop_cols).drop_duplicates()
        new_cols = []
        for col in data.columns:
            if "gmv_amount_this_year" in col:
                new_cols.append(col.split("_")[0])
            else:
                new_cols.append(col)
        data.columns = new_cols
        dates_raw = data.columns[9:].map(int)
        
        # OUTPUT DATES
        self.data = data
        self.calender = calender
        self.dates = calender[calender['WMT Week'].isin(dates_raw.to_numpy())]['Start Date'].values
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are ", end="")
        b = []
        for date in self.dates:
            year, month, day = date.astype('datetime64[D]').astype(str).split('-')
            b.append(f"{day}-{month}-{year}")
        print(b)
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        # if date in self.dates:
        return_date = f"{date.day}-{date.month}-{date.year}"
        week = self.calender.query(f"`Start Date` == '{date}'")['WMT Week'].values[0]
        sales_sum = self.data[str(int(week))].sum()
        return {return_date: ("Walmart", round(sales_sum,2))}
        # else:
        #     raise Exception("Date not found in the given data")
        # sales_sum = self.data['SCANNED_RETAIL_DOLLARS_CUR'].sum()
        # sales_sum = self.data.query(f"`Reporting Period`=='{date}'")['Ordered Revenue'].sum().round(2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# walmart = Walmart(r"./builtin/New Weekly Sales - 01-02-25 ( Walmart ).xlsx", r"./builtin/calender/Walmart Weeks.xlsx")
# walmart.get_data()
# walmart.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Walgreens

# CELL ********************

class Walgreens:
    def __init__(self, file_path):
        '''prep the varibles and general information'''
        
        self.file_path = file_path
        if file_path[-4:] == 'xlsx':
            self.file_format = 'excel'
        elif file_path[-3:] == 'csv':
            self.file_format = 'csv'
        else:
            self.file_format = 'unknown'

    def get_data(self):
        '''fetch the data from the file and prep it for processing, include all preprocessing steps here'''
        # LOAD FILE
        if self.file_format == 'excel':
            data = pd.read_excel(self.file_path)
        if self.file_format == 'csv':
            data = pd.read_csv(self.file_path)
        if self.file_format == 'unknown':
            raise Exception("Unknown File Format")

        # PREP DATA
        data = data.dropna(how='all')
        data.reset_index(inplace=True)
        data.drop(columns=['index'], axis=1, inplace=True)
        date = data.iloc[0,1]
        month, day, year = [int(i) for i in date.split(" ")[-1].split("/")]
        data.drop(index=0, inplace=True)
        data.columns = data.iloc[0].values
        data.drop(index=1, inplace=True)

        print()
        
        # OUTPUT DATES
        self.data = data
        self.dates = [datetime.date(year, month, day)]
        print(f"Dates found in file '{self.file_path.split('/')[-1]}' are {[f'{date.day}-{date.month}-{date.year}' for date in self.dates]}")
    
    def fetch_data(self, date):
        '''date has to be a datetime.date object, fetch the sum of sales for that date'''
        sales_sum = self.data['Total Sales Amount'].sum()
        return_date = f"{date.day}-{date.month}-{date.year}"
        return {return_date: ("Walgreens", round(sales_sum,2))}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# data.drop(columns=['index'], axis=1, inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # data.reset_index(inplace=True)
# date = data.iloc[0,1]
# month, day, year = [int(i) for i in date.split(" ")[-1].split("/")]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# walgreen = Walgreen(r"./builtin/Item Level Weekly $ and Units 02.01.25.xlsx")
# walgreen.get_data()
# walgreen.fetch_data(date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main

# CELL ********************

import pandas as pd
output_df = pd.DataFrame(columns=sources, index=retailers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# raw files
retailer_objects = []

retailer_objects.append(CVS(raw_file_paths['CVS']))
retailer_objects.append(Kroger(raw_file_paths['Kroger']['data'], raw_file_paths['Kroger']['calender']))
retailer_objects.append(Walmart(raw_file_paths['Walmart']['data'], raw_file_paths['Walmart']['calender']))
retailer_objects.append(Amazon(raw_file_paths['Amazon']))
retailer_objects.append(TargetOnline(raw_file_paths['TargetOnline']))
retailer_objects.append(TargetStore(raw_file_paths['TargetStore']))
retailer_objects.append(RiteAid(raw_file_paths['RiteAid']))
retailer_objects.append(Walgreens(raw_file_paths['Walgreens']))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for obj in retailer_objects:
    obj.get_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

retailer_objects[1].data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for obj in retailer_objects:
    temp = f"{date.day}-{date.month}-{date.year}"
    a = obj.fetch_data(date)[formatted_date]
    output_df.loc[a[0], 'Excel'] = a[1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

retailers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

a = get_data_from_sql([date], retailers)[formatted_date]
for i in a:
    output_df.loc[i[0], 'SQL'] = i[1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

a = get_data_from_semantic_model([date], retailers)[formatted_date]
for i in a:
    output_df.loc[i[0], 'PBI'] = i[1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Output

# CELL ********************

output_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

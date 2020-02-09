# generate the csv as output

import pymysql
import pandas as pd
import csv
from jobbank.config import *

import os
try:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))
    start_date = input('please enter the start date in the format of yyyy-mm-dd')
    # start_date = "2019-11-11"
    end_date = input('please enter the end date in the format of yyyy-mm-dd')
    # end_date = "2019-11-30"
    con = pymysql.connect(db_host, db_user, db_password, db_name)
    cursor = con.cursor()
    try:
        get_data = f"select * from {db_output_table} where process_date > '{start_date}'  and '{end_date}' > process_date"
        df = pd.read_sql(get_data,con)
        provinces = (list(pd.unique(df['province'])))
        for province in provinces:
            df_province = df[df['province'] == province ]
            cities = (list(pd.unique(df_province['city'])))

            for city in cities:
                df_city_result = df_province[df_province['city'] == city]
                export_path = os.path.join(BASE_DIR, "export", province,city,'')
                if not os.path.exists(export_path):
                    os.makedirs(export_path)
                f_path = f"{export_path}{today}.csv"
                df_city_result.to_csv(f_path,index=False,quoting=csv.QUOTE_ALL)
                print(f"output file generated at : {f_path}")
    except Exception as e:
        print(e)
except Exception as e:
    print(e)
    print("please enter proper date")
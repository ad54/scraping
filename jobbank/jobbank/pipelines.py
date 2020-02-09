# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv
from jobbank.config import *
import pymysql
from jobbank.items import JobbankItem
import pandas as pd

class JobbankPipeline(object):

    insert_count =0

    def process_item(self, item, spider):
        if isinstance(item,JobbankItem):
            try:
                field_list = []
                value_list = []
                for field in item:
                    if item[field]:
                        field_list.append(str(field))
                        value_list.append(str(item[field]).replace("'",'`'))

                fields = ','.join(field_list)
                values = "','".join(value_list)
                insert_db = "insert into " + db_output_table + "( " + fields + " ) values ( '" + values + "' )"
                spider.cursor.execute(insert_db)
                spider.con.commit()
                self.insert_count += 1
                print('\rnumber of data inserted... ', self.insert_count)
            except Exception as e:
                print('problem in Data insert ', str(e))
        return item

    def close_spider(self,spider):
        try:
            get_data = f"select * from {db_output_table} where process_date = '{today}'"
            df = pd.read_sql(get_data, spider.con)
            file_path = f"{output_directory}job_bank_{today}.csv"
            df.to_csv(file_path,index=False, quoting =csv.QUOTE_ALL)
            print(f"output file generated at : {file_path}")
        except Exception as e:
            print(e)
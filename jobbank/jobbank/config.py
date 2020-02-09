"""Configurations of database, and directory path"""

import platform
import os
import datetime

today = str(datetime.datetime.today()).split()[0]
db_host = "localhost"
db_user = "database_username"
db_password = "database_password"
db_name = "database_name"
db_output_table = "data_table"

# to save the html response
is_save_html = True

# to save the html response as pdf
is_save_pdf = False

# generate directory
current_directory = os.path.dirname(os.path.abspath(__file__))
current_directory = str(current_directory).split('jobbank_script')[0]

if str(platform.system()).lower() == 'windows':
    html_data_directory = f"{current_directory}html\\{today}\\"
    pdf_directory = f"{current_directory}pdf\\{today}\\"
    output_directory = f"{current_directory}output\\{today}\\"
    data_directory = f"{current_directory}data\\"
    export_directory = f"{current_directory}export\\"
else:
    html_data_directory = f"{current_directory}html/html_data/{today}/"
    pdf_directory = f"{current_directory}pdf/{today}/"
    output_directory = f"{current_directory}output/{today}/"
    data_directory = f"{current_directory}data/"
    export_directory = f"{current_directory}export/"

if not os.path.exists(html_data_directory):
    os.makedirs(html_data_directory)
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
if not os.path.exists(data_directory):
    os.makedirs(data_directory)
if not os.path.exists(pdf_directory):
    os.makedirs(pdf_directory)

import os
from datetime import date, datetime, timedelta
import pandas as pd
import requests
import io
import struct
import logging
import json
import sqlalchemy as sa
from urllib.parse import quote_plus

# Define your schema here
schema = 'dev_sahan'

clockify_api_key = "MGEwN2Y3NjAtYzI0Yy00MDFlLWEzY2UtNTM2YzYxYjdlZDM3"

# Output converter for datetimeoffset values
def handle_datetimeoffset(dto_value):
    tup = struct.unpack("<6hI2h", dto_value)
    return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
                    timedelta(hours=tup[7], minutes=tup[8]))

# Set headers
def set_headers(apiKey: str) -> dict:
    headers = {
        'X-Api-Key': apiKey,
        "content-type": "application/json"
    }
    return headers

# Get all workspace list from Clockify
def get_all_workspaces(apiKey: str) -> json:
    headers = set_headers(apiKey)
    url = "https://api.clockify.me/api/v1/workspaces"
    response = requests.get(url, headers=headers)
    WorkspaceResponse = response.json()

    if response.status_code == 200:
        logging.info(f'M:get_all_workspaces, S:Successful')
        print(f'M:get_all_workspaces, S:Successful')
    else:
        logging.info(f'M:get_all_workspaces, S:Failed C:{response.status_code}')
        print(f'M:get_all_workspaces, S:Failed C:{response.status_code}')

    return WorkspaceResponse

# Get detailed report
def get_employee_time_data(workspaceId: str, startDate: date, endDate: date, apiKey: str) -> pd.DataFrame:
    start_date = startDate.strftime("%Y-%m-%d") + "T00:00:00"
    end_date = endDate.strftime("%Y-%m-%d") + "T23:59:59"
    headers = set_headers(apiKey)
    url = f'https://reports.api.clockify.me/v1/workspaces/{workspaceId}/reports/detailed'
    body = {
        "dateRangeStart": start_date,
        "dateRangeEnd": end_date,
        'sortOrder': 'DESCENDING',
        'description': '',
        'rounding': False,
        'withoutDescription': False,
        'amountShown': 'EARNED',
        'zoomLevel': 'YEAR',
        'userLocale': 'en_US',
        'customFields': None,
        'detailedFilter': {
            'sortColumn': 'DATE',
            'page': 1,
            'pageSize': 200,
            'auditFilter': None,
            'quickbooksSelectType': 'ALL'
        },
        "exportType": "csv"
    }
    response = requests.post(url, headers=headers, json=body)

    print(f"API Response Status Code: {response.status_code}")
    print(f"API Response Text: {response.text}")

    if response.status_code == 200:
        dataframe = pd.read_csv(io.StringIO(response.text))
        dataframe['Workspace Id'] = workspaceId
        dataframe['Retrieved Date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f'M:get_employee_time_data, S:Successful, WID:{workspaceId}')
        print(f'M:get_employee_time_data, S:Successful, WID:{workspaceId}')
        return dataframe
    else:
        logging.info(f'M:get_employee_time_data, S:Failed, C:{response.status_code}')
        print(f'M:get_employee_time_data, S:Failed, C:{response.status_code}')
        return None

def upload_data_to_database(dataframe):
    # Set up the database connection for PostgreSQL
    password = "postgres@123"  # Replace with your actual password
    encoded_password = quote_plus(password)
    postgres_connection_string = (
        f"postgresql://postgres:{encoded_password}@194.233.89.26:5432/dev"
    )
    postgres_engine = sa.create_engine(postgres_connection_string)

    # Upload the DataFrame to the PostgreSQL database
    table_name = "landing_clockify"  # Table name in the format "schema_name.table_name"
    dataframe.to_sql(table_name, postgres_engine, schema=schema, if_exists="replace", index=False)

    print(f"{len(dataframe)} rows uploaded to the '{table_name}' table in the '{schema}' schema.")

# Example usage: 
start_date = date(2023, 1, 1)  # Replace with your desired start date
end_date = date(2023, 8, 1)    # Replace with your desired end date

workspaces_list = get_all_workspaces(clockify_api_key)
dataframes = []
for workspace in workspaces_list:
    workspace_id = workspace['id']
    employee_time_data = get_employee_time_data(workspace_id, start_date, end_date, clockify_api_key)
    if employee_time_data is not None and not employee_time_data.empty:
        dataframes.append(employee_time_data)

if dataframes:
    merged_dataframe = pd.concat(dataframes, ignore_index=True)
    upload_data_to_database(merged_dataframe)

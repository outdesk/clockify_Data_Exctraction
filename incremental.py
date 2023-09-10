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
def get_all_workspaces(apiKey: str) -> list:
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
def get_employee_time_data(workspaceId: str, startDate: datetime, endDate: datetime, apiKey: str) -> pd.DataFrame:
    start_date = startDate.strftime("%Y-%m-%dT%H:%M:%S")
    end_date = endDate.strftime("%Y-%m-%dT%H:%M:%S")
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

def setup_database_connection():
    # Set up the database connection for PostgreSQL
    postgres_connection_string = (
        f"postgresql://postgres:{quote_plus('postgres@123')}@194.233.89.26:5432/dev"
    )
    engine = sa.create_engine(postgres_connection_string)
    return engine

# Get the last retrieved date from the database
def get_last_retrieved_date_in_db():
    engine = setup_database_connection()
    with engine.begin() as conn:
        query = sa.text(f"SELECT MAX(\"Retrieved Date\") FROM {schema}.landing_clockify")
        result = conn.execute(query).fetchone()
        last_retrieved_date_in_db_str = result[0] if result[0] else None
        return last_retrieved_date_in_db_str

# Example usage:
last_retrieved_date_in_db = get_last_retrieved_date_in_db()
if last_retrieved_date_in_db:
    start_date = datetime.strptime(last_retrieved_date_in_db, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
else:
    start_date = datetime(2023, 1, 1)
end_date = datetime.now()

# Check if start date and end date are equal
if start_date == end_date:
    print("Table is up to date.")
else:
    # Get employee time data and create DataFrame
    workspaces_list = get_all_workspaces(clockify_api_key)
    dataframes = []
    for workspace in workspaces_list:
        workspace_id = workspace['id']
        employee_time_data = get_employee_time_data(workspace_id, start_date, end_date, clockify_api_key)
        if employee_time_data is not None and not employee_time_data.empty:
            dataframes.append(employee_time_data)

    # Concatenate DataFrames
    combined_dataframe = pd.concat(dataframes, ignore_index=True)

    # Remove headers
    combined_dataframe = combined_dataframe.iloc[1:]

    # Upload the DataFrame to the database
    engine = setup_database_connection()
    table_name = "landing_clockify"  # Table name in the format "schema_name.table_name"
    combined_dataframe.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)

    print(f"{len(combined_dataframe)} rows uploaded to the '{table_name}' table in the '{schema}' schema.")

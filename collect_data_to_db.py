from datetime import datetime, timedelta, timezone, date
import struct
import pyodbc
import requests
import json
import pandas
import io
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta

import config
import logging

clockify_api_key = config.CLOCKIFY_API_KEY
default_start_date = config.DEFAULT_START_DATE

azure_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={config.SERVER};'
    f'DATABASE={config.DATABASE};'
    f'UID={config.UID};'
    f'PWD={config.PWD};'
)

cursor = azure_conn.cursor()


# Output converter for datetimeoffset values
def handle_datetimeoffset(dto_value):
    tup = struct.unpack("<6hI2h", dto_value)
    return datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
                    timezone(timedelta(hours=tup[7], minutes=tup[8])))


# Print all data from 
def get_all_data_from_workspaces_clockify():

    azure_conn.add_output_converter(-155, handle_datetimeoffset)
    # Execute the SQL query to fetch all data from the table
    query = "SELECT * FROM workspaces_clockify"
    cursor.execute(query)

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Process the retrieved data
    for row in rows:
        # Access the columns by index or by name
        id = row[0]
        workspace_id = row[1]
        workspace_name = row[2]
        modified_on = row[3]
        created_on = row[4]

        logging.info(f"ID: {id}, WorkspaceId: {workspace_id}, WorkspaceName: {workspace_name}, modified_on: {modified_on}, created_on: {created_on}")
        print(f"ID: {id}, WorkspaceId: {workspace_id}, WorkspaceName: {workspace_name}, modified_on: {modified_on}, created_on: {created_on}")


#########################################################
# Add workspace data row to the workspaces_clockify table
def create_workspace_data_row(workspace_id, workspace_name):
    is_exist = if_exist_in_workspaces_clockify(workspace_id)
    if not is_exist:
        now_time = datetime.now()

        # Execute the SQL query to insert data
        query = "INSERT INTO workspaces_clockify (WorkspaceId, workspaceName, modified_on, created_on) VALUES (?, ?, ?, ?)"
        params = (workspace_id, workspace_name, now_time, now_time)
        cursor.execute(query, params)
        azure_conn.commit()

        logging.info(f'M:create_workspace_data_row, S:Added data with {workspace_id}, {workspace_name}')
        print(f'M:create_workspace_data_row, S:Added data with {workspace_id}, {workspace_name}')
    

# Check if data exist in workspaces_clockify table
def if_exist_in_workspaces_clockify(workspace_id) -> bool:
    query = f"SELECT COUNT(*) AS count FROM workspaces_clockify WHERE workspaceId = ?"
    params = (workspace_id,)
    cursor.execute(query, params)

    # Get the count value from the query result
    row = cursor.fetchone()
    count = row.count

    # Determine if data exists based on the count value
    if count > 0:
        logging.info(f'M:if_exist_in_workspaces_clockify, S:Data Exist, V:{default_start_date}')
        print(f'M:if_exist_in_workspaces_clockify, S:Data Exist V:{default_start_date}')
        return True
    else:
        logging.info(f'M:if_exist_in_workspaces_clockify, S:Data Does Not Exist')
        print(f'M:if_exist_in_workspaces_clockify, S:Data Does Not Exist')
        return False


# Set headers
def set_headers(apiKey: str) -> str:

    headers = {
        'X-Api-Key': apiKey,
        "content-type": "application/json"
    }
    return headers


# Get all worskspace list from clockify
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


# Save workspaces in workspaces_clockify table
def save_workspace_on_db():
    workSpaces = {}
    workSpacesResponse = get_all_workspaces(clockify_api_key)
    if "message" in workSpacesResponse:
        logging.info(f'M:save_workspace_on_db, E:{workSpacesResponse["message"]}')
        print(f'M:save_workspace_on_db, E:{workSpacesResponse["message"]}')
    
    for worksapceData in workSpacesResponse:
            workSpaces.update({worksapceData['id']: worksapceData['name']})

    for workspace_Id, workspaceName in workSpaces.items():
        create_workspace_data_row(workspace_Id, workspaceName)
        
    return workSpaces


################################
# Save employee time logged data
def save_employee_time_data(workspace_id: str):
    start_date = get_latest_date()
    if start_date is None:

        start_date = datetime.strptime( default_start_date, "%Y-%m-%d").date()
        end_date = date.today()

        for d in rrule( freq= MONTHLY, dtstart=start_date, until=end_date):
            start_date_in_loop = d
            end_date_in_loop = d + relativedelta(months=1)
            logged_time_data = get_employee_time_data(workspace_id, start_date_in_loop, end_date_in_loop, clockify_api_key)
            if logged_time_data is not None: insert_time_data_to_db(logged_time_data, workspace_id)
    else:
        end_date = (date.today() + relativedelta(days=1)).strftime("%Y-%m-%d")
        logged_time_data = get_employee_time_data(workspace_id, start_date, end_date, clockify_api_key)


# Insert eployee logged time data
def insert_time_data_to_db(time_data_df: pandas.DataFrame, workspace_id: str):
    for _, row in time_data_df.iterrows():
        create_time_data_row(row, workspace_id)


# Create empoyee time data row to the landing_clockify table
def create_time_data_row(row, workspace_id: str):
    is_exist = if_exist_in_timedata_clockify(
        workspace_id, 
        row['Email'], 
        datetime.strptime(row['Start Date'], "%d/%m/%Y").strftime("%Y-%m-%d"), 
        row['Start Time'], 
        row['End Time']
        )
    if not is_exist:
        now_time = datetime.now()

        # Execute the SQL query to insert data
        query = '''INSERT INTO landing_clockify (
            Project, 
            Client, 
            Description, 
            Task, 
            Email, 
            Billable, 
            StartDate, 
            StartTime,  
            EndDate,  
            EndTime,  
            DurationH,  
            DurationD,  
            BillableRateUSD,  
            BillableAmountUSD,  
            WorkspaceId, 
            modified_on, 
            created_on ) 
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

        params = (
            str(row['Project']), 
            str(row['Client']),
            str(row['Description']), 
            str(row['Task']), 
            str(row['Email']),
            str(row['Billable']),
            datetime.strptime(row['Start Date'], "%d/%m/%Y").strftime("%Y-%m-%d"),
            row['Start Time'],
            datetime.strptime(row['End Date'], "%d/%m/%Y").strftime("%Y-%m-%d"),
            row['End Time'],
            row['Duration (h)'],
            row['Duration (decimal)'],
            row['Billable Rate (USD)'],
            row['Billable Amount (USD)'],
            workspace_id, 
            now_time, 
            now_time)
        
        cursor.execute(query, params)
        azure_conn.commit()

        # print(f'M:create_time_data_row, S:Added data with {workspace_id}') 

def if_exist_in_timedata_clockify(workspace_id: str, email: str, start_date: datetime, start_time: str, end_time: str):
    query = f'''SELECT COUNT(*) AS count FROM landing_clockify WHERE WorkspaceId = ? AND Email = ? AND StartDate = ? AND StartTime = ? AND EndDate = ?'''
    params = (workspace_id, email, start_date, start_time, end_time)
    cursor.execute(query, params)


# return lates date in landing_clockify table start date
def get_latest_date():
    # Execute the SQL query to retrieve the latest date
    query = "SELECT MAX(startDate) FROM landing_clockify"
    cursor.execute(query)

    # Fetch the result
    result = cursor.fetchone()

    # Process the retrieved date
    latest_date = result[0]
    logging.info(f'M:get_latest_date, V:{latest_date}')
    print(f'M:get_latest_date, V:{latest_date}')


# Get detailed report
def get_employee_time_data(workspaceId: str, startDate: date, endDate: date, apiKey: str) -> pandas.DataFrame:

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
    response = requests.post(url,  headers=headers, json=body)

    if response.status_code == 200:
        dataframe = pandas.read_csv(io.StringIO(response.text))
        print(dataframe.columns)
        dataframe['Worksapce Id'] = workspaceId
        logging.info(f'M:get_employee_time_data, S:Successful, WID:{workspaceId}')
        print(f'M:get_employee_time_data, S:Successful, WID:{workspaceId}') 
        return dataframe
    else:
        logging.info(f'M:get_employee_time_data, S:Failed, C:{response.status_code}')
        print(f'M:get_employee_time_data, S:Failed, C:{response.status_code}')
        return None


def sync_db_with_new_data():
    workspaces_list = save_workspace_on_db()
    for workspace_id, _ in workspaces_list.items():
        save_employee_time_data(workspace_id)


##################
def delete_data():
    # query = "DELETE FROM workspaces_clockify"  
    query = "DELETE FROM landing_clockify"
    cursor.execute(query)

    # Commit the transaction to save the changes
    azure_conn.commit()

    # Get the number of affected rows
    affected_rows = cursor.rowcount

    # Print the number of affected rows
    logging.info(f"{affected_rows} row(s) deleted")
    print(f"{affected_rows} row(s) deleted")



# delete_data()
sync_db_with_new_data()
# get_all_data_from_workspaces_clockify()
# get_latest_date()


import pyodbc

import config


azure_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={config.SERVER};'
    f'DATABASE={config.DATABASE};'
    f'UID={config.UID};'
    f'PWD={config.PWD};'
)

cursor = azure_conn.cursor()


def create_landing_clockify_table():
    if not  check_if_table_available("landing_clockify"):
        cursor.execute(''' CREATE TABLE "landing_clockify" (
            "id" bigint NOT NULL PRIMARY KEY IDENTITY(1,1),
            "Project" text,
            "Client" varchar(100),
            "Description" text,
            "Task" text,
            "User" varchar(100),
            "Group" varchar(100),
            "Email" varchar(254) NOT NULL,
            "Tags" text,
            "Billable" varchar(100),
            "StartDate" date NOT NULL,
            "StartTime" time NOT NULL,
            "EndDate" date NOT NULL,
            "EndTime" time NOT NULL,
            "DurationH" varchar(100) NOT NULL,
            "DurationD" numeric(9, 4) NOT NULL,
            "BillableRateUSD" numeric(9, 4),
            "BillableAmountUSD" numeric(9, 4),
            "WorkspaceId" varchar(100) NOT NULL,
            "modified_on" datetimeoffset NOT NULL,
            "created_on" datetimeoffset NOT NULL
            );
        ''')
        cursor.execute('commit')
        print(f'M: create_landing_clockify_table, S:landing_clockify has created')
    else:
        print(f'M: create_landing_clockify_table, S:landing_clockify already exist')


def create_workspaces_clockify_table():
    if not  check_if_table_available("workspaces_clockify"):
        cursor.execute('''
            CREATE TABLE "workspaces_clockify" (
                "id" bigint NOT NULL PRIMARY KEY IDENTITY(1,1),
                "WorkspaceId" varchar(100) NOT NULL, 
                "WorkspaceName" varchar(100) NOT NULL, 
                "modified_on" datetimeoffset NOT NULL,
                "created_on" datetimeoffset NOT NULL
                );
            ''')

        cursor.execute('commit')
        print(f'M: create_workspaces_clockify_table, S:workspaces_clockify has created')
    else:
        print(f'M: create_workspaces_clockify_table, S:workspaces_clockify already exist')


def show_available_tables():
    print(f'M: show_available_tables, S:Printing available tables')
    cursor.execute('''
                select schema_name(t.schema_id) as schema_name,
                    t.name as table_name,
                    t.create_date,
                    t.modify_date
                from sys.tables t
                order by schema_name,
                        table_name;
            ''')

    row = cursor.fetchone() 
    while row:
        print (row) 
        row = cursor.fetchone()

def check_if_table_available(tablename):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        return True
    return False
 

######################################################
create_landing_clockify_table()
create_workspaces_clockify_table()
show_available_tables()


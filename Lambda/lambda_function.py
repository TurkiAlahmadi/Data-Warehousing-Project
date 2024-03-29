import os
import toml
import requests
import snowflake.connector as sf

# define source of configuration variables
config = toml.load('config.toml')

def get_data(url, file_path):
    """
    get inventory data from the url source and store it into /tmp directory.

    :param url: link to data source
    :param file_path: path to store the data file (/tmp/inventory.csv)
    """
    
    # get inventory data file from url
    try:
        response = requests.get(url)
        print(response.raise_for_status())
        
        with open(file_path, "wb") as file:
            file.write(response.content)
        print("data stored successfully")
    
    except requests.exceptions.HTTPError as e:
        print("HTTP error occurred:", e)

def connect_to_snowflake():
    """
    establish a connection to Snowflake using the provided configuration parameters.
    
    :return: connection object if successful, otherwise None
    """

    # get parameters
    user = os.environ["USER"]
    password = os.environ["PASSWORD"]
    account = os.environ["ACCOUNT"]
    warehouse = config['sf']['warehouse']
    database = config['sf']['database']
    schema = config['sf']['schema']
    role = config['sf']['role']
    
    # create connection object
    try:
        conn = sf.connect(user=user, password=password, account=account, warehouse=warehouse,
                      database=database, schema=schema, role=role)
        print("Successfully connected to Snowflake")
        return conn
    
    except Exception as e:
        print("Failed to connect to Snowflake:", e)
        return None

def upload_data_to_snowflake(conn, file_path):
    """
    upload data from the specified file to Snowflake.

    :param conn: Snowflake connection object
    :param file_path: path to the file containing the data
    """

    # get parameters
    warehouse = config['sf']['warehouse']
    database = config['sf']['database']
    schema = config['sf']['schema']
    file_format_name = config['sf']['file_format_name']
    stage_name = config['sf']['stage_name']
    table = config['sf']['table']
    file_name = config['data']['file_name']

    # run Snowflake queries
    cursor = conn.cursor()
    try:
        cursor.execute(f"use warehouse {warehouse};")
        cursor.execute(f"use {database}.{schema};")
        cursor.execute(f"create or replace file format {file_format_name} type='csv' field_delimiter=',';")
        cursor.execute(f"create or replace stage {stage_name} file_format={file_format_name};")
        cursor.execute(f"put file://{file_path} @{stage_name};")
        cursor.execute(f"list @{stage_name};")
        cursor.execute(f"truncate table {schema}.{table};")
        cursor.execute(f"copy into {schema}.{table} from @{stage_name}/{file_name} file_format={file_format_name} on_error='continue';")

        conn.commit()
        print("Snowflake queries were run successfully")
    
    except Exception as e:
        print("Snowflake queries failed:", e)
    
    finally:
        cursor.close()

def lambda_handler(event, context):
    """
    Lambda main function.
    """
    
    # data parameters
    url = config['data']['url']
    destination_folder = config['data']['dest_folder']
    file_name = config['data']['file_name']
    file_path = os.path.join(destination_folder, file_name)

    # fetch and store data
    get_data(url, file_path)

    # connect to Snowflake
    conn = connect_to_snowflake()
    
    if conn:
        # upload data to Snowflake
        upload_data_to_snowflake(conn, file_path)
        
        # close Snowflake connection
        conn.close()
    
    return {
        'statusCode': 200,
        'body': "data uploaded to Snowflake successfully"
    }
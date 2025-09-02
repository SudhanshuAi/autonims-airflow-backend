import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import boto3
import requests

# Define the directory where DAG files will be stored
dags_folder = "/opt/airflow/dags/"
default_email = "abhranshu.bagchi@gmail.com"


# Helper function to write DAG code to a file
def write_dag_file(dag_code, filename):
    dag_file_path = os.path.join(dags_folder, filename)
    with open(dag_file_path, "w") as file:
        file.write(dag_code)


# **Ingestion DAG Setup Code**
def create_setup_dag_code(child_dag_var, schedule, email, env):
    return f"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import logging

default_args = {{
    'owner': 'airflow',
    'email_on_failure': False,
    'email': ['{email}'],
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
}}

dynamic_var_name = "{child_dag_var}"
config = Variable.get(dynamic_var_name, deserialize_json=True)
dag_id = config.get('dag_id')+"_setup"
sling_api_base_url = Variable.get('SLING_API_URL')
aws_config = Variable.get('AWS_CONFIG', deserialize_json=True)

dag = DAG(
    dag_id,
    default_args=default_args,
    description='Setup DAG for Ingestion',
    schedule_interval='{schedule}',
    catchup=False,
    tags=["setup-ingestion-dag"],
    is_paused_upon_creation=False
)

def call_sling_api(endpoint, payload):
    url = f"{{sling_api_base_url}}{{endpoint}}"
    headers = {{"Content-Type": "application/json"}}
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {{http_err}} - Response content: {{response.text}}")
        raise
    except Exception as err:
        logging.error(f"Error occurred while calling the API: {{err}}")
        raise         

def set_connection(**kwargs):
    conn_config = kwargs['conn_config']
    part_of_workflow = kwargs.get('part_of_workflow')

    details = None
    if part_of_workflow == "true":
        details = {{"type":"s3","bucket": aws_config["AWS_S3_BUCKET_NAME"],"access_key_id":aws_config["AWS_ACCESS_KEY_ID"],"secret_access_key":aws_config["AWS_SECRET_ACCESS_KEY"],"region":aws_config["AWS_REGION"]}}

    if conn_config.get('conn') is not None:
        payload = {{
            "name": conn_config['conn'],
            "details": details if details else conn_config['details'],
            "part_of_workflow": part_of_workflow if part_of_workflow else "No"
        }}
        return call_sling_api("/set_connection", payload)

# --- THIS IS THE FIX ---
# This function is now corrected to pass the entire config object.
def register_job(**kwargs):
    # The 'config' object (read from the Airflow Variable) is passed directly.
    # This ensures that the backend 'app.py' receives the 'config_obj'
    # and all other necessary keys without modification.
    return call_sling_api("/register", config)
# --- END OF THE FIX ---

with dag:
    set_source_connection = PythonOperator(
        task_id='set_source_connection',
        python_callable=set_connection,
        op_kwargs={{'conn_config': config.get('source', {{}}), 'part_of_workflow': config.get('part_of_workflow')}},
        dag=dag
    )

    set_target_connection = PythonOperator(
        task_id='set_target_connection',
        python_callable=set_connection,
        op_kwargs={{'conn_config': config.get('target', {{}})}},
        dag=dag
    )

    register_sling_job = PythonOperator(
        task_id='register_sling_job',
        python_callable=register_job,
        dag=dag
    )

    set_source_connection >> set_target_connection >> register_sling_job
"""


# **Ingestion DAG Run Code**
def create_run_dag_code(child_dag_var, schedule, email, env):
    return f"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)

default_args = {{
    'owner': 'airflow',
    'email_on_failure': False,
    'email': ['{email}'],
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
}}

dynamic_var_name = "{child_dag_var}"
config = Variable.get(dynamic_var_name, deserialize_json=True)
dag_id = config.get('dag_id') + "_run"
sling_api_base_url = Variable.get('SLING_API_URL')
mongodb_config = Variable.get('MONGODB_CONFIG', deserialize_json=True)

dag = DAG(
    dag_id,
    default_args=default_args,
    description='Run Job DAG for Ingestion',
    schedule_interval='{schedule}',
    catchup=False,
    tags=["run-ingestion-dag"],
    is_paused_upon_creation=True
)

def call_sling_api(endpoint, payload):
    url = f"{{sling_api_base_url}}{{endpoint}}"
    headers = {{"Content-Type": "application/json"}}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def run_job(**kwargs):

    master_dag_id = kwargs['dag_run'].conf.get('master_dag_id')
    master_dag_run_id = kwargs['dag_run'].conf.get('master_dag_run_id')

    payload = {{
        "dag_id": config['dag_id'],
        "stream": config['source']['stream'],
        "mongodb_config": mongodb_config,
        "dag_details": {{
            "workflow_dag_id": f"{{master_dag_id}}",
            "workflow_dag_run_id": f"{{master_dag_run_id}}",
            "dag_run_id": kwargs['dag_run'].run_id,
            "run_type": kwargs['dag_run'].run_type,
            "start_date": kwargs['dag_run'].start_date.isoformat(),
            "state": kwargs['dag_run'].state,
            "logical_date": kwargs['logical_date'].isoformat(),
            "last_scheduling_decision": kwargs['dag_run'].last_scheduling_decision.isoformat(),
            "external_trigger": kwargs['dag_run'].external_trigger,
            "execution_date": kwargs['execution_date'].isoformat(),
            "end_date": "",
            "data_interval_end": kwargs['data_interval_end'].isoformat(),
            "data_interval_start": kwargs['data_interval_start'].isoformat(),
            "conf": kwargs['dag_run'].conf,
        }}
    }}

    return call_sling_api("/run", payload)

with dag:
    run_sling_job = PythonOperator(
        task_id='run_sling_job',
        python_callable=run_job,
        dag=dag
    )
"""


# **Transformation DAG Code**
def create_transformation_dag_code(child_dag_var, schedule, email):
    return f"""
import os
import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
import requests
from requests.exceptions import RequestException, ConnectionError
from airflow.exceptions import AirflowException

from datetime import timedelta
import asyncio
import websockets
from pymongo import MongoClient
import json
import pandas as pd
from io import StringIO



default_args = {{
    'owner': 'airflow',
    'email_on_failure': False,
    'email': ['{email}'],
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
}}

dynamic_var_name = "{child_dag_var}"
config = Variable.get(dynamic_var_name, deserialize_json=True)
dag_id = config.get('dag_id') + "_transformation"
edge_backend = Variable.get('edge_backend')
aws_config = Variable.get('AWS_CONFIG', deserialize_json=True)
isPaused = bool(config.get('isPaused', False))

MONGODB_CONFIG = Variable.get("MONGODB_CONFIG", deserialize_json=True)

dag = DAG(
    dag_id,
    default_args=default_args,
    description='Transformation DAG to execute Python code from S3',
    schedule_interval='{schedule}',
    catchup=False,
    is_paused_upon_creation=True,
    tags=["transformation-dag"],
)

# Track logs in a global variable
log_messages = []
log_data = {{}}

def log_message(message):
    log_messages.append({{"timestamp": time.strftime("%Y-%m-%d %H:%M:%S"), "message": f"{{message}}"}})

def upload_data_to_mongodb(MONGODB_CONFIG, data):
    # Establish connection to MongoDB
    connection_string = MONGODB_CONFIG["connection_string"]
    database_name = MONGODB_CONFIG["database_name"]
    collection_name = MONGODB_CONFIG["collection_name"]

    logging.info(f"====================== Adding to DB ========================")

    try:
        client = MongoClient(connection_string)

        db = client[database_name]
        collection = db[collection_name]
        
        result = collection.insert_one(data)
        logging.info(f"============== Inserted document with ID: {{result.inserted_id}} ==================")
        
    except Exception as e:
        print(f"Error uploading data: {{e}}")
    finally:
        client.close()


def upload_result_to_s3(result, file_name):

    logging.info(f"result file ================== {{result}} {{file_name}}")

    df = pd.DataFrame(result)

    region_name=aws_config["AWS_REGION"]
    aws_access_key_id=aws_config["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key=aws_config["AWS_SECRET_ACCESS_KEY"]
    bucket = aws_config["AWS_S3_BUCKET_NAME"]

    # Upload to S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Convert the DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload the CSV to S3
    s3.put_object(
        Bucket=bucket,
        Key=f"{{file_name}}.csv",
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"Uploaded data to S3 bucket {{bucket}} with file name {{file_name}}.")
    logging.info(f"Uploaded data to S3 bucket {{bucket}} with file name {{file_name}}.")

    log_message(f"Uploaded data to S3 bucket {{bucket}} with file name {{file_name}}.")

def fetch_python_code_from_s3(complete_file_url):
    try:
        s3 = boto3.client('s3',
            region_name=aws_config["AWS_REGION"],
            aws_access_key_id=aws_config["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=aws_config["AWS_SECRET_ACCESS_KEY"]
        )
        bucket_name = aws_config["AWS_S3_BUCKET_NAME"]
        obj = s3.get_object(Bucket=bucket_name, Key=complete_file_url)
        log_message(f"Successfully fetched code from S3: {{complete_file_url}}")  # Log message
        return obj['Body'].read().decode('utf-8')
    except Exception as e:
        log_message(f"Error fetching Python code from S3: {{e}}")

        log_data["state"] = "failed"
        # upload_data_to_mongodb(MONGODB_CONFIG, log_data)

        raise

# async def execute_python_code(code, session_id, max_retries=3, delay=5):
#     uri = f"ws://{{edge_backend}}/ws/{{session_id}}"
#     retries = 0

#     while retries < max_retries:
#         try:
#             logging.info(f"=========== code ={{code}}")
#             async with websockets.connect(uri) as websocket:
#                 execute_request = {{
#                     "type": "execute_request",
#                     "code": code
#                 }}
#                 await websocket.send(json.dumps(execute_request))

#                 response = await websocket.recv()
#                 response_data = json.loads(response)

#                 log_message("Successfully executed Python code via WebSocket.")
#                 log_data["state"] = "success"

#                 return response_data
#         except (websockets.exceptions.WebSocketException, ConnectionError) as e:
#             retries += 1
#             log_message(f"WebSocket connection failed: {{e}}. Retrying {{retries}}/{{max_retries}}.")
#             time.sleep(delay)
#             if retries == max_retries:
#                 log_message("Max retries reached. Failing the task.")
#                 log_data["state"] = "failed"
#                 # upload_data_to_mongodb(MONGODB_CONFIG, log_data)
#                 raise

def execute_python_code(code, session_id, max_retries=3, delay=5):
    async def _execute():
        uri = f"ws://{{edge_backend}}/ws/{{session_id}}"
        retries = 0

        while retries < max_retries:
            try:
                async with websockets.connect(uri) as websocket:
                    execute_request = {{
                        "type": "execute_request",
                        "code": code
                    }}
                    await websocket.send(json.dumps(execute_request))

                    response = await websocket.recv()
                    response_data = json.loads(response)

                    log_message("Successfully executed Python code via WebSocket.")
                    log_data["state"] = "success"

                    return response_data
            except (websockets.exceptions.WebSocketException, ConnectionError) as e:
                retries += 1
                log_message(f"WebSocket connection failed: {{e}}. Retrying {{retries}}/{{max_retries}}.")
                time.sleep(delay)
                if retries == max_retries:
                    log_message("Max retries reached. Failing the task.")
                    log_data["state"] = "failed"
                    upload_data_to_mongodb(MONGODB_CONFIG, log_data)
                    raise

    return asyncio.run(_execute())


def on_failure_callback(context):
    log_data["state"] = "failed"
    logging.info(f"Task failed with state: failed")


def run_python_code(**kwargs):
    try:
        master_dag_id = kwargs['dag_run'].conf.get('master_dag_id')
        master_dag_run_id = kwargs['dag_run'].conf.get('master_dag_run_id')

        s3_url = config['s3_file_url']
        user_id = config['user_id']

        logging.info(f"user iddddddd = {{user_id}}")
        complete_file_url = user_id + "/" + s3_url
        logging.info(f"fulll url ==== {{complete_file_url}}")

        log_message(f"Starting to run Python code for user: {{user_id}}")  # Log message
        python_code = fetch_python_code_from_s3(complete_file_url)
        result = execute_python_code(python_code, user_id)

        logging.info(f"result = {{result}}")

        # send the response to s3
        if result:
            logging.info(f"uploading result = {{result}} to s3")
            log_message("uploading result to s3")

            log_message(f"Execution result: {{result}}")  # Log message

            upload_result_to_s3(result, f"transformation_{{master_dag_id}}")


        log_data["workflow_dag_id"] = f"{{master_dag_id}}"
        log_data["workflow_dag_run_id"] = f"{{master_dag_run_id}}"
        log_data["dag_id"] = config['dag_id']
        log_data["dag_run_id"] = kwargs['dag_run'].run_id
        log_data["run_type"] = kwargs['dag_run'].run_type
        log_data["start_date"] = kwargs['dag_run'].start_date.isoformat()
        log_data["logical_date"] = kwargs['logical_date'].isoformat()
        log_data["last_scheduling_decision"] = kwargs['dag_run'].last_scheduling_decision.isoformat()
        log_data["external_trigger"] = kwargs['dag_run'].external_trigger
        log_data["execution_date"] = kwargs['execution_date'].isoformat()
        log_data["data_interval_end"] = kwargs['data_interval_end'].isoformat()
        log_data["data_interval_start"] = kwargs['data_interval_start'].isoformat()
        log_data["logs"] = log_messages
        
    except Exception as e:
        log_data["state"] = "failed"
        log_message(f"Task Failed: {{str(e)}}")

        raise AirflowException(f"Task failed: {{str(e)}}")
    finally:
        # Upload logs to MongoDB

        logging.info("Adding log to db")

        upload_data_to_mongodb(MONGODB_CONFIG, log_data)

        logging.info("Added log to db successfully")
        

with dag:
    run_python_code_task = PythonOperator(
        task_id='run_python_code_task',
        python_callable=run_python_code,
        # retries=3,
        # retry_delay=timedelta(minutes=5),
        on_failure_callback=on_failure_callback
    )
"""


def create_sql_execution_dag_code(child_dag_var, schedule, email):
    return f"""
import os
import logging
import time
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException
import boto3
import requests
from requests.exceptions import RequestException, ConnectionError
from datetime import timedelta

default_args = {{
    'owner': 'airflow',
    'email_on_failure': False,
    'email': ['{email}'],
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
}}

dynamic_var_name = "{child_dag_var}"
config = Variable.get(dynamic_var_name, deserialize_json=True)
dag_id = config.get('dag_id') + "_sql_execution"

dag = DAG(
    dag_id,
    default_args=default_args,
    description='SQL Execution DAG to run queries from S3 JSON file',
    schedule_interval='{schedule}',
    catchup=False,
    tags=["sql-execution-dag"],
)

def fetch_json_from_s3(s3_url, user_id):
    try:
        s3 = boto3.client('s3',
            region_name=Variable.get('AWS_REGION'),
            aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY')
        )
        bucket_name = Variable.get('AWS_S3_BUCKET_NAME')
        obj = s3.get_object(Bucket=bucket_name, Key=user_id + "/" + s3_url)
        json_content = obj['Body'].read().decode('utf-8')
        logging.info("Successfully fetched JSON from S3: %s", s3_url)
        return json.loads(json_content)
    except Exception as e:
        logging.error("Error fetching JSON from S3: %s", e)
        raise

def execute_sql_query(query, db_config, max_retries=3, delay=5):
    url = Variable.get('QUERY_EXECUTION_API_URL').strip()  # URL for your route.ts endpoint
    headers = {{"Content-Type": "application/json"}}
    retries = 0

    payload = {{
        "sqlstr": query,
        "dbtype": db_config['type'],
        "dbConfig": db_config['connection']
    }}

    while retries < max_retries:
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            response.raise_for_status()
            logging.info("Successfully executed SQL query")
            return response.json()
        except (RequestException, ConnectionError) as e:
            retries += 1
            logging.error("HTTP request failed: %s. Retrying %d/%d...", e, retries, max_retries)
            time.sleep(delay)
            if retries == max_retries:
                logging.error("Max retries reached. Failing the task.")
                raise

def run_sql_queries(**kwargs):
    s3_url = config['s3_file_url']
    user_id = config['user_id']
    db_config = config['db_config']
    
    logging.info("Starting to run SQL queries for user: %s", user_id)
    queries = fetch_json_from_s3(s3_url, user_id)
    
    for i, query in enumerate(queries, 1):
        logging.info("Executing query %d of %d, query: %s", i, len(queries), query.get('query'))
        try:
            result = execute_sql_query(query.get('query'), db_config)
            logging.info(f"Query %d executed successfully", i)
        except Exception as e:
            error_msg = str(e)
            logging.error("Error executing query %d: %s", i, error_msg)
            raise AirflowException(f"Query %d failed with error: %e", i, error_msg)
    
    logging.info("All queries executed successfully. Total queries: %d", len(queries))

with dag:
    run_sql_queries_task = PythonOperator(
        task_id='run_sql_queries_task',
        python_callable=run_sql_queries,
        dag=dag
    )
"""


def create_workflow_dag_code(child_dag_var, schedule, email):
    return f"""
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from requests.exceptions import RequestException, ConnectionError
from datetime import timedelta

from airflow.operators.dagrun_operator import TriggerDagRunOperator


default_args = {{
    'owner': 'airflow',
    'email_on_failure': False,
    'email': ['{email}'],
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'is_paused_upon_creation':True
}}

dynamic_var_name = "{child_dag_var}"
config = Variable.get(dynamic_var_name, deserialize_json=True)
dag_id = config.get('dag_id')

ingest_config = config.get('ingest_config')
transform_config = config.get('transform_config')
output_config = config.get('output_config')

# def get_master_dag_details(context, dag_run_obj):
#     # Fetch the current dag_run_id for the master DAG
#     master_dag_id = context['dag'].dag_id
#     master_dag_run_id = context['dag_run'].run_id  # Get the current dag_run_id

#     dag_run_obj.payload = {{'master_dag_id': {{master_dag_id}}, 'master_dag_run_id': {{master_dag_run_id}}}}


dag = DAG(
    dag_id,
    default_args=default_args,
    description='workflow dag description',
    schedule_interval='{schedule}',
    catchup=False,
    tags=["workflow-dag"],
)

def get_conf(**context):
    return {{
        'master_dag_id': context['dag'].dag_id,
        'master_dag_run_id': context['run_id']
    }}

trigger_ingest = TriggerDagRunOperator(
    task_id=f"{{ingest_config["task_id"]}}",
    trigger_dag_id=f"{{ingest_config["trigger_dag_id"]}}",
    wait_for_completion=True,
    conf={{
        'master_dag_id': dag.dag_id,  # Use template
        'master_dag_run_id': '{{{{run_id}}}}'  # Use template
    }},
    dag=dag
)

# Trigger transform Function DAG 
trigger_transform = TriggerDagRunOperator(
    task_id=f"{{transform_config["task_id"]}}",
    trigger_dag_id=f"{{transform_config["trigger_dag_id"]}}",
    wait_for_completion=True,
    conf={{
        'master_dag_id': dag.dag_id,  # Use template
        'master_dag_run_id': '{{{{run_id}}}}'  # Use template
    }},    
    dag=dag
)

trigger_output = TriggerDagRunOperator(
    task_id=f"{{output_config["task_id"]}}",
    trigger_dag_id=f"{{output_config["trigger_dag_id"]}}",
    wait_for_completion=True,
    conf={{
        'master_dag_id': dag.dag_id,  # Use template
        'master_dag_run_id': '{{{{run_id}}}}'  # Use template
    }},    
    dag=dag
)


trigger_ingest >> trigger_transform >> trigger_output

"""


# Retrieve the configuration for the dynamic DAGs from Airflow Variables
dag_configs = Variable.get("parent_dag_var", deserialize_json=True)

# Loop through each configuration and generate the appropriate DAGs
for config in dag_configs["dags"]:
    child_dag_var = config["child_dag_var"]
    child_dag_schedule = config["child_dag_schedule"]
    email = config.get("email", default_email)
    dag_type = config.get("dag_type", "transformation")  # Default to "transformation"
    env = os.getenv("ENV", "development")

    # Generate DAGs based on the type
    if dag_type == "ingestion":
        # Create Setup DAG
        setup_dag_code = create_setup_dag_code(
            child_dag_var, child_dag_schedule, email, env
        )
        write_dag_file(setup_dag_code, f"{child_dag_var}_setup.py")

        # Create Run DAG
        run_dag_code = create_run_dag_code(
            child_dag_var, child_dag_schedule, email, env
        )
        write_dag_file(run_dag_code, f"{child_dag_var}_run.py")

    elif dag_type == "transformation":
        # Create a single Transformation DAG
        transformation_dag_code = create_transformation_dag_code(
            child_dag_var, child_dag_schedule, email
        )
        write_dag_file(transformation_dag_code, f"{child_dag_var}_transformation.py")

    elif dag_type == "sql_execution":
        sql_execution_dag_code = create_sql_execution_dag_code(
            child_dag_var, child_dag_schedule, email
        )
        write_dag_file(sql_execution_dag_code, f"{child_dag_var}_sql_execution.py")

    elif dag_type == "workflow":
        workflow_dag_code = create_workflow_dag_code(
            child_dag_var, child_dag_schedule, email
        )
        write_dag_file(workflow_dag_code, f"{child_dag_var}.py")

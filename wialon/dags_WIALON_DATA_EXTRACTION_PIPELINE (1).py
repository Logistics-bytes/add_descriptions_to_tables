###############################################################################################################

# Author: Findability Sciences 
# Version: 1.0.0
# Description: this script is end to end pipeline of WIALON api to store data from WIALON api to GCS BigQuery.
# Date: 2025-08-11

###############################################################################################################

# Standard Library Imports
import json
import traceback
from datetime import datetime, timedelta, timezone
from io import StringIO
import pytz
import logging
from pathlib import Path
from typing import Dict, Tuple, Any
# Third-Party Library Imports (external but not Airflow/GCP-specific)
import pandas as pd
import pendulum
# Apache Airflow Imports
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.pubsub_operator import PubSubPublishMessageOperator
from airflow.models.param import Param
from airflow.models import Variable

# Google Cloud (GCP) Library Imports
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPIError, NotFound
from airflow.api.client.local_client import Client
import google.auth

# Local/Project-Specific Imports
from utils.WIALON_DATA_EXTRACTION_PIPELINE_FUNCTIONS import load_schema_config_from_gcs,load_sql_query_from_gcs,load_data_to_raw,validate_and_load_data,insert_logs,insert_dataset,calculate_metrics
from utils.LOGGING_HELPER import insert_log_record_into_bigquery, logger_info, logger_error, get_gcs_logger
from utils.ETL_HELPER import generate_elt_id, generate_job_id
from utils.RESOURCE_USAGE_HELPER import log_resource_usage

#get project name
project_id = google.auth.default()[1]

ELT_ID=None
job_id=None


# get dag id which is the name of the current file
dag_id = Path(__file__).stem

# Construct the path to the config file using the DAG ID and a "_CONFIG.json" suffix.
CONFIG_FOLDER = Path(__file__).parent / "config"

#reading global config file
CONFIG_GLOBAL =  json.loads(Variable.get("global_config"))
default_timezone = CONFIG_GLOBAL.get('default_timezone') # Get the default timezone from config (e.g., "America/Guatemala")
bucket_name = CONFIG_GLOBAL.get('bucket') # Get bucket name



# logs are stored in cloud bucket with the same name as current file
log_name = dag_id

date_format = "%Y-%m-%dT%H:%M:%S.%f"

logger = get_gcs_logger(log_prefix=f"logs/{'_'.join(dag_id.split('_')[0:5])}")

def get_current_timestamp(default_timezone: str) -> str:
    """ 
    Returns the current timestamp in ISO format with microseconds.
    Args:
        default_timezone (str): The timezone to use for the current timestamp.
    Returns:    
        str: The current timestamp in ISO format with microseconds.
    """
    if not isinstance(default_timezone, str):
        raise ValueError("default_timezone must be a string")
    try:
        tz = pytz.timezone(default_timezone)
    except pytz.exceptions.UnknownTimeZoneError:
        raise ValueError(f"Invalid timezone: {default_timezone}")
    return datetime.now(tz).strftime(date_format)

def validate_bigquery_config(config: Dict[str, Any]) -> None:
    """Validates the structure and existence of required BigQuery tables defined in the global configuration.
    This function validates the BigQuery configuration section and checks if specified tables exist.
    Args:
        config (dict): Configuration dictionary containing BigQuery settings.
            Expected to have a 'bigquery' key with table configurations.
    Returns:
        None
    Raises:
        ValueError: If 'bigquery' section is not a dictionary or if table IDs are missing/empty.
        RuntimeError: If specified BigQuery tables don't exist or on unexpected BigQuery errors.
    Example:
        config = {
            'bigquery': {
                'table1': 'dataset.table1',
                'table2': 'dataset.table2'
            }
        }
        validate_bigquery_config(config)
    """

    
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary.")
    
    logger_info(logger, log_name, ELT_ID, job_id, "begin: validate_bigquery_config", default_timezone)
    bq_client = bigquery.Client()

    # === Validate BigQuery Section ===
    bigquery_config: Dict[str, str] = config.get("bigquery", {})
    if not isinstance(bigquery_config, dict):
        raise ValueError(
            "Configuration Error: 'bigquery' section must be a dictionary."
        )

    for key, table_id in bigquery_config.items():
        if not table_id:
            raise ValueError(f"Configuration Error: 'bigquery.{key}' is missing or empty.")
        try:
            bq_client.get_table(f"{project_id}.{table_id}")
        except NotFound:
            raise RuntimeError(f"[ERROR] BigQuery table '{table_id}' does not exist.")
        except Exception as e:
            raise RuntimeError(f"[ERROR] Unexpected error for BigQuery table '{table_id}': {e}")
    logger_info(logger, log_name, ELT_ID, job_id, 'end: validate_bigquery_config',default_timezone)


def validate_pubsub_config(config: Dict[str, Any]) -> None:
    """Validates the structure and existence of required Pub/Sub topics defined in the global configuration.
    This function checks if all required Pub/Sub topics exist and are accessible in the Google Cloud project.
    It validates the configuration structure and attempts to get each topic to ensure it exists.
    Args:
        config (dict): Configuration dictionary containing Pub/Sub settings.
            Expected structure:
            {
                "pubsub": {
                    "sub_key": {
                        "key": "topic_id"
                    }
                }
            }
    Raises:
        ValueError: If the configuration structure is invalid or topics are missing.
        RuntimeError: If any Pub/Sub topic is not accessible or unexpected errors occur.
    Examples:
        >>> config = {
        ...     "pubsub": {
        ...         "notifications": {
        ...             "success": "project-success-topic",
        ...             "error": "project-error-topic"
        ...         }
        ...     }
        ... }
        >>> validate_pubsub_config(config)
    """
    
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: validate_pubsub_config', default_timezone)
    
    publisher = pubsub_v1.PublisherClient()
    pubsub_config: Dict[str, Dict[str, str]] = config.get("pubsub", {})
    if not isinstance(pubsub_config, dict):
        raise ValueError("Configuration Error: 'pubsub' section must be a dictionary")


    for sub_key, sub_dict in pubsub_config.items():
        if not isinstance(sub_dict, dict):
            raise ValueError(f"Configuration Error: 'pubsub.{sub_key}' must be a dictionary.")

        for key, topic_id in sub_dict.items():
            if not topic_id:
                raise ValueError(f"Configuration Error: 'pubsub.{sub_key}.{key}' is missing or empty.")

            try:
                topic_path = publisher.topic_path(project_id, topic_id)
                publisher.get_topic(request={"topic": topic_path})
                logger_info(logger, log_name, ELT_ID, job_id,
                            f"[VALIDATED] Pub/Sub topic '{topic_id}' exists.", default_timezone)
            except GoogleAPIError as e:
                raise RuntimeError(f"[ERROR] Pub/Sub topic '{topic_id}' not accessible: {e}")
            except Exception as e:
                raise RuntimeError(f"[ERROR] Unexpected error for Pub/Sub topic '{topic_id}': {e}")

    logger_info(logger, log_name, ELT_ID, job_id, 'end: validate_pubsub_config', default_timezone)

def publish_to_pubsub(data: Dict[str, Any], context: Any, task_id: str, pubsub_type: str, config: Dict[str, Any]) -> None:
    """Publishes a JSON message to a configured Pub/Sub topic.
    This function takes a data payload and publishes it as a JSON message to a Google Cloud Pub/Sub topic, 
    typically used to trigger Cloud Functions for tasks like sending emails or writing logs to BigQuery.
    Args:
        data (dict): The data payload to be published as a JSON message.
        context (dict): The Airflow context containing runtime information.
        task_id (str): Identifier for the Airflow task.
        pubsub_type (str): The type of Pub/Sub message to be published, used to determine the target topic.
        config (dict): Configuration dictionary containing Pub/Sub settings including topics.
    Raises:
        KeyError: If required configuration keys are missing.
        GoogleAPIError: If there's an error interacting with Google Cloud APIs.
        RuntimeError: For any other unexpected errors during message publishing.
    Returns:
        None
    Example:
        ```python
        data = {"message": "example", "timestamp": "2023-01-01"}
        publish_to_pubsub(data, context, "notify_task", "notification", config)
        ```
    """
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("task_id must be a non-empty string")
    if not isinstance(pubsub_type, str) or not pubsub_type:
        raise ValueError("pubsub_type must be a non-empty string")
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary")


    logger_info(logger, log_name, ELT_ID, job_id, 'begin: publish_to_pubsub',default_timezone)
    try:
        pubsub_message = json.dumps(data)
        pubsub_config = config.get("pubsub")
        if not isinstance(pubsub_config, dict):
            raise TypeError("Expected 'pubsub' in config to be a dictionary")
 
        pubsub_type_config = pubsub_config.get(pubsub_type)
        if not isinstance(pubsub_type_config, dict):
            raise KeyError(f"Missing or invalid config for pubsub type: '{pubsub_type}'")
 
        topic = pubsub_type_config.get("topic")
        if not topic:
            raise KeyError(f"'topic' not defined for pubsub type: '{pubsub_type}'")
        operator = PubSubPublishMessageOperator(
			task_id=task_id,
			project_id=project_id,
			topic=topic,
			messages=[{
			    'data': pubsub_message.encode('utf-8')
			}],
			dag=dag,
		    )
        operator.execute(context=context)
        logger_info(logger, log_name, ELT_ID, job_id, f"[SUCCESS] Message published to Pub/Sub topic '{topic}'.",default_timezone)
    except KeyError as ke:
        error_msg = f"KeyError in PubSub message configuration: {ke}"
        logger_error(logger, log_name, ELT_ID, job_id, error_msg,default_timezone)
        raise RuntimeError(error_msg)

    except GoogleAPIError as gae:
        error_msg = f"Google Cloud API error during PubSub publish: {gae}"
        logger_error(logger, log_name, ELT_ID, job_id, error_msg,default_timezone)
        raise RuntimeError(error_msg)

    except Exception as e:
        error_trace = traceback.format_exc()
        error_msg = f"Unexpected error during Pub/Sub publish: {e}\n{error_trace}"
        logger_error(logger, log_name, ELT_ID, job_id, error_msg,default_timezone)
        raise RuntimeError(error_msg)
    
    logger_info(logger, log_name, ELT_ID, job_id, 'end: publish_to_pubsub',default_timezone)

def get_date_range(config: Dict[str, Any])-> Tuple[str, str, str]:
    """
    Returns the start and end dates used for API calling.

    Args:
        config (Dict[str, Any]): Configuration dictionary containing keys 
            - data_extraction.end_date (str or None)
            - data_extraction.duration (int)
            - data_extraction.delay (int)

    Returns:
        Tuple[str, str, str]: start_date, end_date, and start_date_before_delay in '%Y-%m-%d' format.
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary")
    end_date = config["data_extraction"]["end_date"]
    duration = int(config["data_extraction"]["duration"])
    duration=duration-1
    delay = int(config["data_extraction"]["delay"])

    if end_date is None or end_date == '':
        start_date_before_delay = datetime.now(pytz.timezone(default_timezone)).strftime('%Y-%m-%d')
        end_date = datetime.now(pytz.timezone(default_timezone)) - timedelta(days=int(delay))

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        start_date_before_delay = end_date.strftime('%Y-%m-%d')
        logger_info(logger, log_name, ELT_ID, job_id, f'start_date_before_delay: {start_date_before_delay}',default_timezone)

    start_date = (end_date - timedelta(days=duration)).strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    return start_date, end_date,start_date_before_delay

# getting start date, end date timestamp needed for api call 
def get_date_range_timestamp(start_date: str, end_date: str) -> Tuple[int, int]:
    """
    Converts start and end dates into Unix timestamps for API usage.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        Tuple[int, int]: Start and end Unix timestamps.
    """
    if not isinstance(start_date, str) or not isinstance(end_date, str):
        raise TypeError("start_date and end_date must be strings in format YYYY-MM-DD")
    start_date_obj = datetime.strptime(f'{start_date} 06:00:00 GMT+0000',"%Y-%m-%d %H:%M:%S GMT%z")
    logger_info(logger, log_name, ELT_ID, job_id, f'start date in gmt: {start_date_obj}',default_timezone)
    start_timestamp=int(start_date_obj.timestamp())
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    end_date_obj = datetime.strptime(
        f'{end_date_obj.strftime("%Y-%m-%d")} 05:59:59 GMT+0000',
        "%Y-%m-%d %H:%M:%S GMT%z")
    logger_info(logger, log_name, ELT_ID, job_id, f'end date(changed) in gmt {end_date_obj}',default_timezone)
    end_timestamp = int(end_date_obj.timestamp())
    logger_info(logger, log_name, ELT_ID, job_id, f'start timestamp: {start_timestamp}',default_timezone)
    logger_info(logger, log_name, ELT_ID, job_id, f'end timestamp: {end_timestamp}',default_timezone)
    
    guatemala_tz = timezone(timedelta(hours=-6))
    
    guatemala_time = datetime.fromtimestamp(start_timestamp, tz=guatemala_tz)
    dt1=guatemala_time.strftime("%Y-%m-%d %H:%M:%S")
    guatemala_time = datetime.fromtimestamp(end_timestamp, tz=guatemala_tz)
    dt2=guatemala_time.strftime("%Y-%m-%d %H:%M:%S")
    
    logger_info(logger, log_name, ELT_ID, job_id, f'start timestamp converted too guatemalla time: {dt1}',default_timezone)
    logger_info(logger, log_name, ELT_ID, job_id, f'end timestamp converted too guatemalla time {dt2}',default_timezone)
    return start_timestamp,end_timestamp


def get_file_path(file_name: str) -> str:
    """
    Retrieves the full GCS path of a file and logs its columns.

    Args:
        file_name (str): Name of the file in the bucket.

    Returns:
        str: Full GCS path to the file.
    """     
    if not isinstance(file_name, str):
        raise TypeError("file_name must be a string")   
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    full_file_path=f"gs://{bucket_name}/{file_name}"
    csv_content = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_content))
    logger_info(logger, log_name, ELT_ID, job_id, f'column name: {df.columns}',default_timezone)
    return full_file_path

# load data from buclet to raw and from raw to stage   
def load_raw_and_stage_data(schema_name: str,scripts_name: str,full_file_path: str,raw_table: str,stage_table: str,elt_id: str,job_id: str,ti) -> Tuple[int, int]:
    """
    Loads data from a GCS file to BigQuery raw and staging tables.

    Args:
        schema_name (str): Name of the schema file in GCS.
        scripts_name (str): Name of the SQL script for raw-to-stage transformation.
        full_file_path (str): GCS path of the data file.
        raw_table (str): Raw BigQuery table name.
        stage_table (str): Staging BigQuery table name.
        ELT_ID (str): ELT process ID.
        job_id (str): Job ID for logging.
        ti: Airflow task instance object for XCom push.

    Returns:
        Tuple[int, int]: Number of records inserted into raw and staging tables.
    """
    if not all(isinstance(arg, str) for arg in [schema_name, scripts_name, full_file_path, raw_table, stage_table, elt_id, job_id]):
        raise TypeError("All parameters except 'ti' must be strings")
    if ti is None:
        raise ValueError("Task instance 'ti' is required")
    
    logger_info(logger, log_name, elt_id, job_id, 'begin: load_raw_and_stage_data',default_timezone)

    # load schema for the table
    schema_config = load_schema_config_from_gcs(logger, log_name, elt_id, job_id, schema_name, default_timezone)
    # load sql query for the table needed for raw to stage transformation
    sql_query = load_sql_query_from_gcs(logger, log_name, elt_id, job_id, scripts_name, default_timezone)    
    # inserted into bigquery raw table
    input_records=load_data_to_raw(logger, schema_config, job_id, full_file_path, raw_table, log_name, elt_id, default_timezone)

    # validating input records
    if input_records == 0:
        raise ValueError("No records to process. input_records cannot be 0.")

    # needed for sql query
    current_date = datetime.now(pytz.timezone(default_timezone)).strftime("%Y-%m-%d") 

    # inserted into bigquery stage table
    output_records=validate_and_load_data(logger, sql_query, schema_config, job_id, raw_table, stage_table, log_name, elt_id, default_timezone, current_date)

    # validating output records
    # if output_records == 0:
    #     raise ValueError(f"No records to process. output_records cannot be 0. input record is {input_records}")
    # elif output_records != input_records:
    #     raise ValueError(f"records didn't match. output_records {output_records} not same as input_records {input_records}")

    ti.xcom_push(key='storage_bucket_records', value=input_records)
    ti.xcom_push(key='bigquery_records', value=output_records)

    logger_info(logger, log_name, elt_id, job_id, f'input_records: {input_records}',default_timezone)
    logger_info(logger, log_name, elt_id, job_id, f'output_records: {output_records}',default_timezone)
    return input_records,output_records


def get_log_link(start_time: str) -> str:
    """Gets a formatted log link for Google Cloud Console logging.

    This function generates an HTML link to Google Cloud Console logs for a specific DAG execution.
    It handles time conversion between local Guatemala time and UTC for proper log retrieval.

    Args:
        start_time (str): The start time in Guatemala timezone in the format defined by date_format

    Returns:
        str: An HTML formatted link that directs to Google Cloud Console logs within a 2-hour window 
            (1 hour before and after the start time) for the specified DAG execution

    Example:
        >>> start_time = "2023-11-14 10:00:00"
        >>> get_log_link(start_time)
        'to see detailed logs click this link - <a href="https://console...">here</a>'

    Note:
        The function assumes the following global variables are defined:
        - date_format: The datetime format string
        - default_timezone: The timezone string for Guatemala
        - project_id: The Google Cloud project ID
        - dag_id: The Airflow DAG ID
        - ELT_ID: The ELT process identifier
    """
    if not isinstance(start_time, str):
        raise ValueError("start_time must be a string in the format defined by date_format")

    start_time_cst = datetime.strptime(start_time, date_format)
    guatemala_tz = pytz.timezone(default_timezone)
    start_time_with_tz = guatemala_tz.localize(start_time_cst)
    utc_time = start_time_with_tz.astimezone(pytz.utc)
    cursortimestamp = utc_time.strftime(date_format) + 'Z'
    utc_time_minute_one_hour = utc_time + timedelta(hours=-1)
    starttime = utc_time_minute_one_hour.strftime(date_format) + 'Z'
    utc_time_plus_one_hour = utc_time + timedelta(hours=1)
    endtime = utc_time_plus_one_hour.strftime(date_format) + 'Z'
    logs_link=f'to see detailed logs click this link - <a href="https://console.cloud.google.com/logs/query;query={log_name};cursorTimestamp={cursortimestamp};startTime={starttime};endTime={endtime}?project={project_id}">here</a>'
    return logs_link


def task_error_format(task_error: str) -> str:
    """Formats error messages for safe usage in Pub/Sub message payloads.

    This function takes an error message string and sanitizes it by removing special characters
    and formatting it for safe transmission in Pub/Sub messages.

    Args:
        task_error (str): The error message string to be formatted.

    Returns:
        str: A cleaned and sanitized version of the error message with:
            - Single quotes replaced with spaces
            - Newlines replaced with spaces 
            - Backslashes removed
            - Enclosed in string representation (via repr())

    Example:
        >>> error = "Error: file 'test.txt'\n not found\\here"
        >>> task_error_format(error)
        'Error: file test.txt not found here'
    """
    if not isinstance(task_error, str):
        raise ValueError("task_error must be a string")
    task_error = task_error.replace("'", " ")
    task_error = task_error.replace("\n", " ")
    task_error = repr(task_error)
    task_error = task_error.replace('\\', '')
    task_error = task_error.replace("'", " ")
    return task_error

def read_config_fn():
    """Reads configuration from a JSON file specified in the DAG run configuration.
    This function attempts to read a configuration file path from the DAG run context.
    If found, it loads the JSON config file. If the file is not found or no config
    is specified in the DAG run, returns an empty dictionary.
    The function also handles logging of the start, end and config contents using
    both local and cloud logging.
    Returns:
        dict: The configuration dictionary loaded from the JSON file, or an empty
            dictionary if no config file was specified or found.
    Raises:
        FileNotFoundError: If the specified config file path does not exist.
            This is caught internally and returns empty dict.
    Note:
        The function expects a dag_run context with optional 'config_file' parameter
        specifying the path to a JSON configuration file.
    """
    

    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: read_config_fn',default_timezone)
    context = get_current_context()
    dag_run = context.get("dag_run") #check if the dag is triggered by a master dag
    
    if dag_run.conf:
        #reading config file from master dag or from thhe UI
        CONFIG_PATH = dag_run.conf.get("config_file")
        try:
            with open(fr'{CONFIG_PATH}', 'r') as json_file:
                config = json.load(json_file)
        except FileNotFoundError:
            config = {}  # or load default_config()
    else:
        config = {}
    logger_info(logger, log_name, ELT_ID, job_id, f'config read is: {config}',default_timezone)
    logger_info(logger, log_name, ELT_ID, job_id, 'end: read_config_fn',default_timezone)
    return config

# Function to update FS_ELT_LOG when the DAG starts
@task
def start_dag_fn():
    """Initializes the ELT job run, validates configuration, and logs the start of the DAG.
    This function handles the initialization of an Extract-Load-Transform (ELT) job by setting up
    logging, generating or retrieving a unique ELT ID, validating configurations, and logging
    task-specific metadata into BigQuery tables.
    The function performs the following main operations:
    1. Sets up cloud logging
    2. Generates or retrieves ELT_ID
    3. Reads and validates configuration
    4. Logs job start information to BigQuery
    5. Validates BigQuery and Pub/Sub configurations
    6. Publishes start status to Pub/Sub
    Returns:
        None
    Raises:
        ValueError: If the configuration file is missing or empty
        Exception: For any other errors during execution, which are logged and re-raised
    Note:
        - The function uses XCom to push various values for downstream tasks
        - Logs are written to both Cloud Logging and BigQuery tables
        - The function expects certain environment variables and configurations to be present
    """
    
 

    ELT_ID=None
    job_id=None

    logger_info(logger, log_name, ELT_ID, job_id, 'begin: start_dag',default_timezone)

    context = get_current_context()
    ti = context['ti']
    
    start_time = get_current_timestamp(default_timezone) # Convert to ISO format for BigQuery
    ti.xcom_push(key='start_time', value=start_time) #pushing to xcom
    
    error_in_start_dag = ''

    log_resource_usage('start_dag', logger, log_name, ELT_ID, job_id, default_timezone) #prints memory related information
    

    dag_run = context.get("dag_run") #check if the dag is triggered by a master dag
    if dag_run.conf:
        ELT_ID = dag_run.conf.get("ELT_ID")   # get ELT ID from the master
    if ELT_ID is None:
        ELT_ID = generate_elt_id(logger)  # Generate a random ELT ID
    logger_info(logger, log_name, ELT_ID, job_id, f"run id :- {ELT_ID} ",default_timezone)


    end_time = None
    try:
        config = read_config_fn()#  get the config file
        if not config:
            error_in_start_dag = "[ERROR] Missing or empty config in DAG"
            raise ValueError(error_in_start_dag)
        logger_info(logger, log_name, ELT_ID, job_id, f"[INFO] Config loaded successfully in start_dag_fn: {config}",default_timezone)

        ti.xcom_push(key='config', value=config) #pushing to xcom
        ti.xcom_push(key='ELT_ID', value=ELT_ID)

        elt_table_name = config["bigquery"]["raw_table"].split(".")[-1]
        ti.xcom_push(key='elt_table_name', value=elt_table_name)
        
        # logging task specific details in bigquery log table FS_ELT_JOB_RUN_WM
        initial_message = {
                "ELT_JOB_ID": 'None',
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "start_dag",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": None,
                "ELT_JOB_STATUS": "started",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": None,
                "ELT_OUTPUT_RECORDS": None,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": start_time,
                "DATA_LOAD_STAGE": None,
                "RECORD_COUNT": None
            }
        table = config.get("bigquery", {}).get("elt_job_table")
        if table is None:
            raise ValueError("Missing 'elt_job_table' in 'bigquery' config ")
        insert_log_record_into_bigquery(initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        
        # Validates the existence and structure of required BigQuery tables and Pub/Sub topics as defined in the config.
        validate_bigquery_config(config)
        validate_pubsub_config(config)

        # logging elt id to bigquery log table FS_ELT_LOG
        data = {
                'ELT_ID': ELT_ID,
                'status': 'started',
                'project_id': project_id,
                'bucket_name': bucket_name,
                'config_file': config
            }

        publish_to_pubsub(data, context, 'start_dag_wrapped', 'pubsub_log', config)

        end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
        sucess_message = {
                "ELT_JOB_ID": 'None',
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "start_dag",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": end_time,
                "ELT_JOB_STATUS": "success",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": None,
                "ELT_OUTPUT_RECORDS": None,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": start_time,
                "DATA_LOAD_STAGE": None,
                "RECORD_COUNT": None
            }

        insert_log_record_into_bigquery(sucess_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        logger_info(logger, log_name, ELT_ID, job_id, "start_dag Function Executed",default_timezone)
    except Exception as e:
        if not error_in_start_dag:
            error_in_start_dag = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, error_in_start_dag,default_timezone)
        ti.xcom_push(key='task_name', value='start_dag')
        ti.xcom_push(key='task_error', value=str(error_in_start_dag))
        raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: start_dag',default_timezone)

# Function to clean up FS_ELT_LOG when the DAG ends
@task
def end_dag():
    """Performs cleanup steps for the ELT pipeline by removing ELT_ID and logging completion.
    This function handles the final steps of the DAG execution:
    - Retrieves the ELT_ID and configuration from XCom
    - Publishes completion status to PubSub
    - Logs resource usage and completion status
    - Handles any errors during cleanup
    Returns:
        None
    Raises:
        ValueError: If ELT_ID is None
        Exception: If any other error occurs during cleanup
    Example:
        >>> end_dag()
        # Performs cleanup and publishes completion status to PubSub
    """
      
    
    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: end_dag',default_timezone)
    context = get_current_context()
    ti = context['ti']
    config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task
    
    log_resource_usage('end_dag', logger, log_name, ELT_ID, job_id, default_timezone)

    error_in_end_dag = ''
    try:
        ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')  # Pull the ELT_ID from XCom
        if ELT_ID is None:
            error_in_end_dag = "ELT_ID is None"
            raise ValueError(error_in_end_dag)
        # deleting elt id from bigquery log table FS_ELT_LOG
        data = {'ELT_ID':ELT_ID,
                    'status':'ended',
                    'project_id': project_id,
                    "bucket_name":bucket_name,
                    "config_file":config}

        publish_to_pubsub(data, context, 'end_dag_wrapped', 'pubsub_log', config)

    except Exception as e:
        if not error_in_end_dag:
            error_in_end_dag = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, error_in_end_dag,default_timezone)
        ti.xcom_push(key='task_name', value='end_dag')
        ti.xcom_push(key='task_error', value=str(error_in_end_dag))
        raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: end_dag',default_timezone)

# Function to insert initial record into FS_ELT_JOB_RUN_WM
@task
def initialize_job_run():
    """Creates a new job ID and logs ELT process metadata to BigQuery.
    This function initializes a new ELT job run by:
    1. Creating a unique job ID 
    2. Logging initial metadata about the job to BigQuery logging table
    3. Pushing job ID and start time to XCom for downstream tasks
    Returns:
        None
    Raises:
        Exception: If there are any errors during initialization process, logs error 
            and raises exception
    Note:
        Function expects the following XCom values to be available:
        - ELT_ID from 'start_dag_fn' task
        - config from 'start_dag_fn' task
        Function pushes the following XCom values:
        - ELT_JOB_ID: Unique job identifier
        - start_time: Job start timestamp
        - task_name: On error, set to 'initialize_job_run'
        - task_error: On error, contains error message
    """
    

    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: initialize_job_run',default_timezone)
    context = get_current_context()
    ti = context['ti']

    start_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery

    log_resource_usage('initialize_job_run', logger, log_name, ELT_ID, job_id, default_timezone)

    error_in_initialize_job_run = ''
    try:
        ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')  # Pull the ELT_ID from XCom
        config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task
        elt_table_name = ti.xcom_pull(task_ids='start_dag_fn', key='elt_table_name')  # Pull the elt table name from XCom
        elt_start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time') # Pull the elt start time from XCom

        job_id = generate_job_id(logger)  # Generate a random JOB ID

        initial_message = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "initialize_job_run",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": None,
                "ELT_JOB_STATUS": "started",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": None,
                "ELT_OUTPUT_RECORDS": None,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": None,
                "RECORD_COUNT": None
            }
        table = config.get("bigquery", {}).get("elt_job_table")
        if table is None:
            raise ValueError("Missing 'elt_job_table' in 'bigquery' config ")
        insert_log_record_into_bigquery(initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        logger_info(logger, log_name, ELT_ID, job_id, f"ELT ID - {ELT_ID}, Job ID - {job_id}",default_timezone)
        
        end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
        success_message = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "initialize_job_run",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": end_time,
                "ELT_JOB_STATUS": "success",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": None,
                "ELT_OUTPUT_RECORDS": None,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": None,
                "RECORD_COUNT": None
            }

        insert_log_record_into_bigquery(success_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        logger_info(logger, log_name, ELT_ID, job_id, f"Loaded job_id: {job_id} into BigQuery table.",default_timezone)
        ti.xcom_push(key='ELT_JOB_ID', value=job_id)
        ti.xcom_push(key='start_time', value=start_time)

    except Exception as e:
        if not error_in_initialize_job_run:
            error_in_initialize_job_run = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, error_in_initialize_job_run,default_timezone)
        ti.xcom_push(key='ELT_JOB_ID', value=job_id)
        ti.xcom_push(key='task_name',  value='initialize_job_run')
        ti.xcom_push(key='task_error', value=str(error_in_initialize_job_run))
        ti.xcom_push(key='start_time', value=start_time)
        raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: initialize_job_run',default_timezone)


@task
def api_to_gcs():
    """Orchestrates the process of extracting data from WIALON API and storing it in Google Cloud Storage.
    This function handles the complete workflow of data extraction from WIALON API to GCS, including
    configuration validation, logging setup, Cloud Run job execution, and error handling.
    Returns:
        None
    Raises:
        ValueError: If the configuration is None.
        Exception: For any errors during the execution process.
    The function performs the following main steps:
        1. Validates configuration and gets job identifiers
        2. Sets up logging and dataset parameters
        3. Executes Cloud Run job for data extraction
        4. Handles success/failure scenarios
        5. Logs process details in BigQuery
    Note:
        This function requires proper configuration setup and appropriate GCP credentials.
        It uses Cloud Logging for comprehensive logging and BigQuery for process tracking.
    """   
    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: api_to_gcs',default_timezone)
    context = get_current_context()
    ti = context['ti']
    api_to_gcs_error = ''
    process_name = 'api_to_gcs'
    process_log_id = None
    end_time = None
    job_duration = None
    process_id = None
    user_id = None
    dataset_id = None
    process_log_id = None

    start_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
    
    log_resource_usage('api_to_gcs', logger, log_name, ELT_ID, job_id, default_timezone)
    
    api_to_gcs_error = ''
    job_id = ti.xcom_pull(task_ids='initialize_job_run', key='ELT_JOB_ID')
    ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')  # Pull the ELT_ID from XCom
    config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task
    elt_table_name = ti.xcom_pull(task_ids='start_dag_fn', key='elt_table_name')  # Pull the elt table name from XCom
    elt_start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time') # Pull the elt start time from XCom

    try:
        if job_id is None:
            api_to_gcs_error = "JOB_ID is None"
            logger_error(logger, log_name, ELT_ID, job_id, "api_to_gcs_error",default_timezone)
            raise ValueError(api_to_gcs_error)
        
        initial_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": "api_to_gcs",
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME":  None,
            "ELT_JOB_STATUS": "started",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": None,
            "ELT_OUTPUT_RECORDS": None,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        table = config.get("bigquery", {}).get("elt_job_table")
        if table is None:
            raise ValueError("Missing 'elt_job_table' in 'bigquery' config.")
        insert_log_record_into_bigquery(initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        
        JOB_NAME = config["cloud_run"]["cloud_run_job_name"]
        region = config["cloud_run"]["region"]

        # get start date, end date
        start_date,end_date,start_date_before_delay = get_date_range(config)

        # getting table name, file name/path      
        file_path = config["data_extraction"]["file_path"]
        table_name = config["bigquery"]["raw_table"].split('.')[-1]

        schema_name = f"gs://{file_path}/{start_date_before_delay.replace('-','')}/"
        if start_date==end_date:
            file_path=f"{file_path}/{start_date_before_delay.replace('-','')}/{table_name}_{start_date.replace('-','')}.csv"
            dataset_name = f"{table_name}_{start_date.replace('-','')}.csv"
        else:
            file_path=f"{file_path}/{start_date_before_delay.replace('-','')}/{table_name}_{start_date.replace('-','')}_{end_date.replace('-','')}.csv"
            dataset_name = f"{table_name}_{start_date.replace('-','')}.csv"
        
        
        logger_info(logger, log_name, ELT_ID, job_id, f'file_path: {file_path}',default_timezone)

        # logging table to maintain process logs and details of  datasets - insert rows
        try:
            process_id, user_id, _, _, _, process_log_id,error_in_insert_log = insert_logs(config=config, start_time=start_time, end_time=end_time, process_status='Running',
                                                                                                                                    error_message=api_to_gcs_error,job_duration=job_duration,process_name=process_name,process_log_id=process_log_id, dataset_id = dataset_id, default_timezone = default_timezone, logger = logger, 
log_name = log_name, elt_id = ELT_ID, job_id = job_id)
            dataset_id = insert_dataset(config = config, process_id = process_id, user_id = user_id, platform = 'GCP-GCS', schema = schema_name, dataset_name = dataset_name, dataset_type = 'CSV', logger = logger,
            log_name = log_name, elt_id = ELT_ID, job_id = job_id)
            api_to_gcs_error = error_in_insert_log
            logger_info(logger, log_name, ELT_ID, job_id, f"process_log_id: {process_log_id}",default_timezone)
        except Exception as e:
            api_to_gcs_error = f"Unexpected error during logging prerequisites: {e}"
            raise
        
        # getting start date, end date timestamp needed for api call 
        start_timestamp, end_timestamp = get_date_range_timestamp(start_date,end_date)

        location = config["data_extraction"]["location"]
        # calling container which calls api and store data as a csv to a bucket path
        logger_info(logger, log_name, ELT_ID, job_id, f"""gcloud run jobs execute {JOB_NAME} --wait --region {region} --update-env-vars FROM_TIME={start_timestamp},TO_TIME={end_timestamp},DAG_ID={dag_id},ELT_ID={ELT_ID},JOB_ID={job_id},FILE_PATH={file_path},TABLE_NAME={table_name},BUCKET_NAME={bucket_name},LOCATION={location},ELT_TABLE_NAME={elt_table_name},ELT_START_TIME={elt_start_time}""",default_timezone)
        operator =  BashOperator(
            task_id='api_to_gcs_wrapped',
            bash_command=f"""gcloud run jobs execute {JOB_NAME} --wait --region {region} --update-env-vars FROM_TIME={start_timestamp},TO_TIME={end_timestamp},DAG_ID={dag_id},ELT_ID={ELT_ID},JOB_ID={job_id},FILE_PATH={file_path},TABLE_NAME={table_name},BUCKET_NAME={bucket_name},LOCATION={location},ELT_TABLE_NAME={elt_table_name},ELT_START_TIME={elt_start_time}""",
            dag=dag,
        )
        logger_info(logger, log_name, ELT_ID, job_id, "Executing bash operator",default_timezone)
        operator.execute(context=context)


        end_time = get_current_timestamp(default_timezone)
        success_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": "api_to_gcs",
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME":  end_time,
            "ELT_JOB_STATUS": "success",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": None,
            "ELT_OUTPUT_RECORDS": None,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        insert_log_record_into_bigquery(success_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
    
    except Exception as e:
        if not api_to_gcs_error:
            api_to_gcs_error = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, api_to_gcs_error,default_timezone)
        ti.xcom_push(key='task_name', value='api_to_gcs')
        ti.xcom_push(key='task_error', value=str(api_to_gcs_error))
        raise  # Reraise the exception to mark the task as failed
    finally:
        # logging table to maintain process logs and details of  datasets - update rows
        try:
            process_id, user_id, _, _, _, process_log_id,error_in_insert_log = insert_logs(config=config, start_time=start_time, end_time=end_time, process_status='Success' if (api_to_gcs_error=='' or api_to_gcs_error is None) else 'Error',
                                                                                                                                   error_message=api_to_gcs_error,job_duration=job_duration,process_name=process_name,process_log_id=process_log_id, dataset_id = dataset_id, default_timezone = default_timezone, logger = logger,
log_name = log_name, elt_id = ELT_ID, job_id = job_id)

        except Exception as e:
            api_to_gcs_error = f"Unexpected error during logging prerequisites: {e}"
            raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: api_to_gcs',default_timezone)


@task
def gcs_to_bigquery():
    """Transfers data from Google Cloud Storage to BigQuery with logging and error handling.
    This function performs an ETL process by reading data from GCS, transforming it, and loading
    it into BigQuery. It handles logging of the process status, metrics calculation, and error tracking.
    Args:
        None: All required parameters are pulled from the Airflow context and config
    Returns:
        None: Results are pushed to XCom for downstream task consumption
    Raises:
        Exception: If any error occurs during the ETL process
    Process Flow:
        1. Initializes logging and context
        2. Reads CSV file from GCS
        3. Performs ETL process (load raw â†’ transform â†’ validate)
        4. Logs process metrics and status
        5. Calculates final metrics
    Note:
        - Requires Airflow context with proper configuration
        - Logs are written to both Cloud Logging and BigQuery
        - Process metrics are tracked for input and output record counts
    """   
    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: gcs_to_bigquery',default_timezone)
    context = get_current_context()
    ti = context['ti']
    input_records=0
    output_records=0
    gcs_to_bigquery_error = ''
    process_name = 'gcs_to_bigquery'
    process_log_id = None
    end_time = None
    job_duration = None
    process_id = None
    user_id = None
    user_code = None
    dataset_id = None
    log_resource_usage('gcs_to_bigquery', logger, log_name, ELT_ID, job_id, default_timezone)

    start_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
    config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task

    # logging table to maintain process logs and details of  datasets - insert rows
    try:
        process_id, user_id, user_code, _, _, process_log_id,error_in_insert_log = insert_logs(config=config, start_time=start_time, end_time=end_time, process_status='Running',
                                                                                                                               error_message=gcs_to_bigquery_error,job_duration=job_duration,process_name=process_name,process_log_id=process_log_id, dataset_id = dataset_id, default_timezone = default_timezone, logger = logger,
log_name = log_name, elt_id = ELT_ID, job_id = job_id)

        gcs_to_bigquery_error = error_in_insert_log
    except Exception as e:
        gcs_to_bigquery_error = f"Unexpected error during logging prerequisites: {e}"
        raise

    try:
        ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')
        job_id = ti.xcom_pull(task_ids='initialize_job_run', key='ELT_JOB_ID')
        elt_table_name = ti.xcom_pull(task_ids='start_dag_fn', key='elt_table_name')  # Pull the elt table name from XCom
        elt_start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time') # Pull the elt start time from XCom
        initial_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": "gcs_to_bigquery",
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": None,
            "ELT_JOB_STATUS": "started",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        table = config.get("bigquery", {}).get("elt_job_table")
        if table is None:
            raise ValueError("Missing 'elt_job_table' in 'bigquery' config.")
        insert_log_record_into_bigquery( initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)

        # getting start date and end date needed to read a file from a bucket
        start_date,end_date,start_date_before_delay = get_date_range(config)

        # getting file name and table name needed to load data from a bucket path to a given bigquery table
        file_path   = config["data_extraction"]["file_path"]
        table_name = config["bigquery"]["raw_table"].split('.')[-1]

        # two file name here as container stores csv file for both HC_DISATEL_CONDUCCION_EFICIENTE_REPORT and DEV_HC_DISATEL_CONDUCCION_EFICIENTE_REPORT_VALUES
        if start_date==end_date:
            file_name       =f"{file_path}/{start_date_before_delay.replace('-','')}/{table_name}_{start_date.replace('-','')}.csv"
            file_name_table2=f"{file_path}/{start_date_before_delay.replace('-','')}/{table_name}_VALUES_{start_date.replace('-','')}.csv"
        else:
            file_name       =f"{file_path}/{start_date_before_delay.replace('-','')}/{table_name}_{start_date.replace('-','')}_{end_date.replace('-','')}.csv"
            file_name_table2=f"{file_path}/{start_date_before_delay.replace('-','')}/{table_name}_VALUES_{start_date.replace('-','')}_{end_date.replace('-','')}.csv"
        
        # for table HC_DISATEL_CONDUCCION_EFICIENTE_REPORT
        logger_info(logger, log_name, ELT_ID, job_id, f'file_path, file_name:  {file_path} {file_name}',default_timezone)

        # get path in bucket for this table
        full_file_path = get_file_path(file_name)


        # load data from bucket to raw and from raw to stage   
        input_records,output_records = load_raw_and_stage_data(config["data_extraction"]["schema_name"],config["data_extraction"]["scripts_name"],
                                                               full_file_path,config["bigquery"]["raw_table"],
                                                               config["bigquery"]["stage_table"],ELT_ID, job_id,ti)


        end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
        raw_data_count_message_1 = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": "gcs_to_bigquery",
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": None,
            "ELT_JOB_STATUS": "running",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": "gcs_to_raw",
            "RECORD_COUNT": input_records
        }

        insert_log_record_into_bigquery(raw_data_count_message_1, table, logger, log_name, ELT_ID, job_id,default_timezone)

        st_data_count_message_1 = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": "gcs_to_bigquery",
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": None,
            "ELT_JOB_STATUS": "running",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": "raw_to_staging",
            "RECORD_COUNT": output_records
        }

        insert_log_record_into_bigquery(st_data_count_message_1, table, logger, log_name, ELT_ID, job_id,default_timezone)

        success_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": "gcs_to_bigquery",
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": end_time,
            "ELT_JOB_STATUS": "success",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": config["bigquery"]["raw_table"],
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        insert_log_record_into_bigquery(success_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        
        # if second table is present
        if config["bigquery"].get("raw_table2"):

            logger_info(logger, log_name, ELT_ID, job_id, 'gcs_to_bigquery Table 2 Started',default_timezone)
            logger_info(logger, log_name, ELT_ID, job_id, f'file_path and file_name: {file_path} {file_name_table2}',default_timezone)

            # get path in bucket for this table
            full_file_path = get_file_path(file_name_table2)

            # load data from bucket to raw and from raw to stage
            input_records,output_records = load_raw_and_stage_data(config["data_extraction"]["schema_name_table2"],config["data_extraction"]["scripts_name_table2"],
                                                                full_file_path,config["bigquery"]["raw_table2"],
                                                                    config["bigquery"]["stage_table2"],ELT_ID, job_id,ti)

            end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
            elt_table_name = config["bigquery"]["raw_table2"].split(".")[-1]
            raw_data_count_message_2 = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "gcs_to_bigquery",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": None,
                "ELT_JOB_STATUS": "running",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": input_records,
                "ELT_OUTPUT_RECORDS": output_records,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": "gcs_to_raw",
                "RECORD_COUNT": input_records
            }

            insert_log_record_into_bigquery(raw_data_count_message_2, table, logger, log_name, ELT_ID, job_id,default_timezone)

            st_data_count_message_2 = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "gcs_to_bigquery",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": None,
                "ELT_JOB_STATUS": "running",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": input_records,
                "ELT_OUTPUT_RECORDS": output_records,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": "raw_to_staging",
                "RECORD_COUNT": output_records
            }

            insert_log_record_into_bigquery(st_data_count_message_2, table, logger, log_name, ELT_ID, job_id,default_timezone)

            success_message = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "gcs_to_bigquery",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": end_time,
                "ELT_JOB_STATUS": "success",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": input_records,
                "ELT_OUTPUT_RECORDS": output_records,
                "ELT_TABLE_NAME": config["bigquery"]["raw_table2"],
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": None,
                "RECORD_COUNT": None
            }
            insert_log_record_into_bigquery( success_message, table, logger, log_name, ELT_ID, job_id,default_timezone)


    except Exception as e:
        if not gcs_to_bigquery_error:
            gcs_to_bigquery_error = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, f'gcs_to_bigquery_error captured {gcs_to_bigquery_error}',default_timezone)
        ti.xcom_push(key='task_name', value='gcs_to_bigquery')
        ti.xcom_push(key='task_error', value=str(gcs_to_bigquery_error))
        ti.xcom_push(key='storage_bucket_records', value=None)
        raise
    finally:
        # logging table to maintain process logs and details of  datasets - update rows
        try:
            # insert into dataset
            dataset_id = insert_dataset(config = config, process_id = process_id, user_id = user_id, platform = 'GCP-BQ', schema= config["bigquery"]["stage_table"].split(".")[0], dataset_name = config["bigquery"]["stage_table"].split(".")[1], dataset_type = 'TABLE', logger = logger, log_name = log_name, elt_id = ELT_ID, job_id = job_id)
            process_id, user_id, user_code, _, _, process_log_id,error_in_insert_log = insert_logs(config=config, start_time=start_time, end_time=end_time, process_status='Success' if (gcs_to_bigquery_error=='' or gcs_to_bigquery_error is None) else 'Error',
                                                                                                                                   error_message=gcs_to_bigquery_error,job_duration=job_duration,process_name=process_name,process_log_id=process_log_id, dataset_id = dataset_id, default_timezone = default_timezone, logger = logger,
log_name = log_name, elt_id = ELT_ID, job_id = job_id)


            calculate_metrics(config=config,process_log_id=process_log_id,user_code=user_code,default_timezone=default_timezone, logger = logger, 
            log_name = log_name, elt_id = ELT_ID, job_id = job_id)
        except Exception as e:
            gcs_to_bigquery_error = f"Unexpected error during logging prerequisites: {e}"
            raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: gcs_to_bigquery',default_timezone)

def get_fact_table_record_count(
        project_id: str,
        table: str,
        pais: str,
        pais_table_name: str,
        elt_start_time: str,
        logger: logging.Logger,
        log_name: str,
        elt_id: str,
        job_id: str,
        default_timezone: str
    )-> int:
    """
    Fetches the record count from the specified BigQuery fact table.
    Args:
        project_id (str): The GCP project ID.
        table (str): Table in the format 'dataset.table_name'.
        pais (str): Country filter value.
        pais_table_name (str): Table name containing country information.
        elt_start_time (str): ELT start time.
        logger (Logger): logger instance.
        log_name (str): Log name for Cloud Logging. 
        elt_id (str): ELT identifier.
        job_id (str): Job ID identifier.
        default_timezone (str): Default timezone.
    
    Returns:
        int: Row count of the specified table.
    """
    try:
        dataset_id = table.split(".")[0]
        table_name = table.split(".")[1]
        start_datetime = datetime.strptime(elt_start_time, "%Y-%m-%dT%H:%M:%S.%f")

        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        query = """
            SELECT COUNT(*) AS row_count
            FROM `{project_id}.{dataset_id}.{table_name}` f
            WHERE f.FECHA_PARTICION >= @start_datetime
            AND EXISTS (
            SELECT 1
            FROM `{project_id}.{dataset_id}.{pais_table_name}` p
            WHERE p.SK_PAIS = f.SK_PAIS
            AND p.PAIS = @pais
        )
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_datetime", "DATETIME", start_datetime),
                bigquery.ScalarQueryParameter("pais", "STRING", pais)
            ]
        )
        # Execute query
        query_job = client.query(query.format(project_id=project_id, dataset_id=dataset_id, table_name=table_name, pais_table_name=pais_table_name), job_config=job_config)
        result = list(query_job.result())

        if not result:
            logger_error(logger, log_name, elt_id, job_id, f"No results returned from fact-{table} record count query execution", default_timezone)
            raise RuntimeError(f"No results returned from fact-{table} record count query execution")

        return result[0].row_count

    except Exception as e:
        logger_error(logger, log_name, elt_id, job_id, f"Error fetching fact record count for {table}: {e}", default_timezone)
        raise e

@task
def store_procedure():
    """
    Executes one or more BigQuery stored procedures and logs their execution details.

    This task:
    - Retrieves the ELT_ID and ELT_JOB_ID from XCom.
    - Logs job metadata (start time, job type, etc.) into a BigQuery log table.
    - Executes stored procedures configured in the Airflow DAG configuration (via `start_dag_fn`).
    - Logs success/failure status, records affected, and error messages if any.
    - Pushes execution metadata to Airflow's XCom for downstream use.

    Assumptions:
    - `start_dag_fn` and `initialize_job_run` tasks have completed and pushed relevant context.
    - The `config` dictionary contains:
        - `bigquery.elt_job_table` for logging.
        - `data_extraction.dim_fact_stored_procs` as a list of stored procedure references.
    - Stored procedures are valid and callable in BigQuery.
    - Logging utilities and `insert_log_record_into_bigquery()` are properly defined elsewhere.
    - The environment has access to Google Cloud Logging and BigQuery APIs.

    Side Effects:
    - Executes BigQuery stored procedures.
    - Inserts job metadata into a BigQuery logging table.
    - Publishes logs to Cloud Logging.
    - Pushes keys to XCom: `task_name`, `task_error`, `storage_bucket_records`.

    Raises:
        Exception: If any part of the stored procedure execution or logging fails.

    """
    context = get_current_context()
    ti = context['ti']
    config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task
    ELT_ID=None
    job_id=None

    data_extract = config.get("data_extraction")
    if not isinstance(data_extract, dict):
        raise KeyError("'data_extraction' config section is missing or invalid.")
    
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: store_procedure_fn')
    start_time = get_current_timestamp(default_timezone)

    logger_info(logger, log_name, ELT_ID, job_id, "Executing store procedure task", default_timezone)
    log_resource_usage('store_procedure_fn', logger, log_name, ELT_ID, job_id,default_timezone)
    input_record_count = 0
    output_record_count = 0
    store_procedure_error = ''

    try:
        try:
            ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')
            job_id = ti.xcom_pull(task_ids='initialize_job_run', key='ELT_JOB_ID')
            elt_table_name = ti.xcom_pull(task_ids='start_dag_fn', key='elt_table_name') # Pull the elt table name from XCom
            elt_start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time') # Pull the elt start time from XCom

            initial_message = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "store_procedure",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": None,
                "ELT_JOB_STATUS": "started",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": input_record_count,
                "ELT_OUTPUT_RECORDS": output_record_count,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": None,
                "RECORD_COUNT": None
            }
            
            table=config['bigquery']['elt_job_table']
            insert_log_record_into_bigquery( initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)

            # Check if stored procedures are defined in the config
            if "dim_fact_stored_procs" in config["data_extraction"] :
                logger_info(logger, log_name, ELT_ID, job_id, "Executing stored procedures")
                for proc in config["data_extraction"]["dim_fact_stored_procs"]:
                    query =  f"CALL {proc};"
                    logger_info(logger, log_name, ELT_ID, job_id,f'Executing query -> {query}')
                    client = bigquery.Client()
                    # Run the query
                    load_job = client.query(query)

                    # Wait for the job to complete
                    load_job.result()

                    # Retrieve the number of rows affected
                    output_record_count = load_job.num_dml_affected_rows
                    logger_info(logger, log_name, ELT_ID, job_id,f'Execution completed -> {query}')

            end_time = get_current_timestamp(default_timezone)

            rows_loaded  = get_fact_table_record_count(
                project_id,
                table=config["bigquery"]["fact_table"],
                pais=config["data_extraction"]["pais"],
                pais_table_name=config["data_extraction"]["pais_table_name"],
                elt_start_time=elt_start_time,
                logger=logger,
                log_name=log_name,
                elt_id=ELT_ID,
                job_id=job_id,
                default_timezone=default_timezone
            )

            logger_info(logger, log_name, ELT_ID, job_id,f'Rows loaded to fact table {config["bigquery"]["fact_table"]} : {rows_loaded}')
            
            if config["bigquery"].get("raw_table2"):
                success_message_1 = {
                    "ELT_JOB_ID": job_id,
                    "ELT_ID": ELT_ID,
                    "ELT_JOB_TYPE": context['dag'].dag_id,
                    "ELT_JOB_NAME": "store_procedure",
                    "ELT_JOB_START_TIME": start_time,
                    "ELT_JOB_END_TIME": end_time,
                    "ELT_JOB_STATUS": "success",
                    "ELT_JOB_ERROR_MESSAGE": None,
                    "ELT_INPUT_RECORDS": input_record_count,
                    "ELT_OUTPUT_RECORDS": output_record_count,
                    "ELT_TABLE_NAME": f"{elt_table_name}_VALUES",
                    "ELT_START_TIME": elt_start_time,
                    "DATA_LOAD_STAGE": "staging_to_fact",
                    "RECORD_COUNT": rows_loaded
                }

                table=config['bigquery']['elt_job_table']
                insert_log_record_into_bigquery( success_message_1, table, logger, log_name, ELT_ID, job_id,default_timezone)


            success_message_2 = {
                "ELT_JOB_ID": job_id,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": context['dag'].dag_id,
                "ELT_JOB_NAME": "store_procedure",
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": end_time,
                "ELT_JOB_STATUS": "success",
                "ELT_JOB_ERROR_MESSAGE": None,
                "ELT_INPUT_RECORDS": input_record_count,
                "ELT_OUTPUT_RECORDS": output_record_count,
                "ELT_TABLE_NAME": elt_table_name,
                "ELT_START_TIME": elt_start_time,
                "DATA_LOAD_STAGE": "staging_to_fact",
                "RECORD_COUNT": rows_loaded
            }

            table=config['bigquery']['elt_job_table']
            insert_log_record_into_bigquery( success_message_2, table, logger, log_name, ELT_ID, job_id,default_timezone)
            
            # publish_and_wait_for_response(success_message, task_instance, kwargs, logger)

        except Exception as e:
            store_procedure_error = f"An error occurred in store_procedure(): {e}"
            raise
    except Exception as e:
        if not store_procedure_error:
            store_procedure_error = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, store_procedure_error)
        ti.xcom_push(key='task_name', value='store_procedure')
        ti.xcom_push(key='task_error', value=str(store_procedure_error))
        ti.xcom_push(key='storage_bucket_records', value=None)
        raise e
    logger_info(logger, log_name, ELT_ID, job_id, 'end: store_procedure_fn')



@task
def mark_job_success():
    """Marks the ELT job as successful in the metadata log table.
    This function logs the successful completion of the ELT job in BigQuery metadata tables.
    It records both initial and final status, along with metrics like input/output record counts.
    The function is designed to run as the final task in the DAG after all data processing is complete.
    Raises:
        Exception: If there are any errors during the logging process, with the error message captured
            and re-raised.
    Returns:
        None
    Example Task Dependencies:
        start_task >> initialize_job_task >> api_to_gcs_task >> gcs_to_bq_task >> mark_job_success
    Note:
        - Requires valid task context with XCom values from previous tasks
        - Logs both to Cloud Logging and BigQuery metadata tables
        - Records metrics like input/output record counts from previous tasks
        - Updates job status to 'success' on completion
    """

    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: mark_job_success',default_timezone)
    context = get_current_context()
    ti = context['ti']

    start_time = get_current_timestamp(default_timezone)
    
    log_resource_usage('mark_job_success', logger, log_name, ELT_ID, job_id, default_timezone)
    
    mark_job_success_error = ''
    try:
        ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')
        job_id = ti.xcom_pull(task_ids='initialize_job_run', key='ELT_JOB_ID')
        config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task
        elt_table_name = ti.xcom_pull(task_ids='start_dag_fn', key='elt_table_name') # Pull the elt table name from XCom
        elt_start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time') # Pull the elt start time from XCom
        initial_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": 'mark_job_success',
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": None,
            "ELT_JOB_STATUS": "started",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": None,
            "ELT_OUTPUT_RECORDS": None,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        ti.xcom_push(key='ELT_JOB_STATUS', value=initial_message['ELT_JOB_STATUS'])
        table = config.get("bigquery", {}).get("elt_job_table")
        if table is None:
            raise ValueError("Missing 'elt_job_table' in 'bigquery' config")
        insert_log_record_into_bigquery(initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)


        # trigger monitoring dag when the job is a success
        monitoring_config = config.get('data_extraction', {}).get('monitoring_config')
        logger_info(logger, log_name, ELT_ID, job_id, f'config path for monitoring is: {monitoring_config }',default_timezone)
        if monitoring_config:
            logger_info(logger, log_name, ELT_ID, job_id, f'triggering started for Monitoring_task with conf: { monitoring_config }',default_timezone)
            
            client = Client(None, None)
            client.trigger_dag(
                dag_id="Monitoring_task",
                conf={"config_file": monitoring_config},
            )
        else:
            logger_info(logger, log_name, ELT_ID, job_id, "No monitoring_config found. Skipping trigger.",default_timezone)


        output_records = ti.xcom_pull(task_ids='gcs_to_bigquery', key='bigquery_records')
        input_records = ti.xcom_pull(task_ids='gcs_to_bigquery', key='storage_bucket_records')
        end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
        success_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": 'mark_job_success',
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": end_time,
            "ELT_JOB_STATUS": "success",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        ti.xcom_push(key='ELT_JOB_STATUS', value=success_message['ELT_JOB_STATUS'])
        ti.xcom_push(key='success_message', value=success_message)
        insert_log_record_into_bigquery(success_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        logger_info(logger, log_name, ELT_ID, job_id, f"Updated job record in FS_ELT_JOB_RUN_WM for ID {ELT_ID} with success status",default_timezone)

    except Exception as e:
        if not mark_job_success_error:
            mark_job_success_error = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, mark_job_success_error,default_timezone)
        raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: mark_job_success',default_timezone)

@task
def mark_job_failure():
    """Marks the ELT job as failure in the metadata log table.
    This function handles failure logging for the ELT pipeline by:
    1. Recording the initial job start in BigQuery
    2. Identifying which task failed and retrieving its error message
    3. Logging the final failure status with error details
    Args:
        None - Uses Airflow context and XCom to get required values
    Returns:
        None
    Raises:
        Exception: If there are any errors during the failure logging process
    Notes:
        - Executed if any preceding tasks in the DAG fails
        - Records job metadata including:
            - ELT_ID
            - Job ID
            - Start/end timestamps
            - Error messages
            - Record counts
        - Logs to both BigQuery and Cloud Logging
        - Uses XCom to pull values from previous tasks:
            - start_dag_fn
            - initialize_job_run  
            - api_to_gcs
            - gcs_to_bigquery
    Example Task Dependencies:
        start_task >> initialize_job_task >> api_to_gcs_task >> gcs_to_bq_task
    """
    

    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: mark_job_failure',default_timezone)
    context = get_current_context()
    ti = context['ti']

    start_time = get_current_timestamp(default_timezone)

    log_resource_usage('mark_job_failure', logger, log_name, ELT_ID, job_id, default_timezone)

    mark_job_failure_error = ''
    try:
        ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')
        job_id = ti.xcom_pull(task_ids='initialize_job_run', key='ELT_JOB_ID')
        job_id = str(job_id) if job_id is not None else str(None)
        config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task
        elt_table_name = ti.xcom_pull(task_ids='start_dag_fn', key='elt_table_name') # Pull the elt table name from XCom
        elt_start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time') # Pull the elt start time from XCom

        input_records = None
        output_records = None
        initial_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": 'mark_job_failure',
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": None,
            "ELT_JOB_STATUS": "started",
            "ELT_JOB_ERROR_MESSAGE": None,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        table = config.get("bigquery", {}).get("elt_job_table")
        if table is None:
            raise ValueError("Missing 'elt_job_table' in 'bigquery' config")
        insert_log_record_into_bigquery(initial_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        # pulling error message from each task to se which one failed
        # the task which failed will have error message other will be empty string 
        start_dag_run_error = ti.xcom_pull(task_ids='start_dag_fn', key='task_name')
        initialize_job_run_error = ti.xcom_pull(task_ids='initialize_job_run', key='task_name')
        api_to_gcs_error = ti.xcom_pull(task_ids='api_to_gcs', key='task_name')
        gcs_to_bigquery_error = ti.xcom_pull(task_ids='gcs_to_bigquery', key='task_name')

        input_records = ti.xcom_pull(task_ids=gcs_to_bigquery_error, key='storage_bucket_records')
        output_records = ti.xcom_pull(task_ids=gcs_to_bigquery_error, key='bigquery_records')

        if start_dag_run_error:
            task_name  = ti.xcom_pull(task_ids=start_dag_run_error, key='task_name')
            task_error = ti.xcom_pull(task_ids=start_dag_run_error, key='task_error')
        elif initialize_job_run_error:
            task_name  = ti.xcom_pull(task_ids=initialize_job_run_error, key='task_name')
            task_error = ti.xcom_pull(task_ids=initialize_job_run_error, key='task_error')
        elif api_to_gcs_error:
            task_name  = ti.xcom_pull(task_ids=api_to_gcs_error, key='task_name')
            task_error = ti.xcom_pull(task_ids=api_to_gcs_error, key='task_error')
        elif gcs_to_bigquery_error:
            task_name  = ti.xcom_pull(task_ids=gcs_to_bigquery_error, key='task_name')
            task_error = ti.xcom_pull(task_ids=gcs_to_bigquery_error, key='task_error')
        else:
            task_name = ti.task_id
            task_error = str(context.get('exception', 'Unknown error'))

        # Format task_error string to ensure it's safe and clean for Pub/Sub message payload
        task_error = task_error_format(task_error)

        end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery

        failed_message = {
            "ELT_JOB_ID": job_id,
            "ELT_ID": ELT_ID,
            "ELT_JOB_TYPE": context['dag'].dag_id,
            "ELT_JOB_NAME": f'mark_job_failure_{task_name}',
            "ELT_JOB_START_TIME": start_time,
            "ELT_JOB_END_TIME": end_time,
            "ELT_JOB_STATUS": "failure",
            "ELT_JOB_ERROR_MESSAGE": task_error,
            "ELT_INPUT_RECORDS": input_records,
            "ELT_OUTPUT_RECORDS": output_records,
            "ELT_TABLE_NAME": elt_table_name,
            "ELT_START_TIME": elt_start_time,
            "DATA_LOAD_STAGE": None,
            "RECORD_COUNT": None
        }
        logger_info(logger, log_name, ELT_ID, job_id, f'failed_message af: {failed_message}',default_timezone)
        ti.xcom_push(key='ELT_JOB_STATUS', value=failed_message['ELT_JOB_STATUS'])
        ti.xcom_push(key='failed_message', value=failed_message)
        insert_log_record_into_bigquery(failed_message, table, logger, log_name, ELT_ID, job_id,default_timezone)
        logger_info(logger, log_name, ELT_ID, job_id, f"Updated job record in FS_ELT_JOB_RUN_WM for ID {ELT_ID} with failed status",default_timezone)

    except Exception as e:
        if not mark_job_failure_error:
            mark_job_failure_error = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, mark_job_failure_error,default_timezone)
        raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: mark_job_failure',default_timezone)

@task
def prepare_mail_task():
    """Prepares and publishes email notification data for ELT job status.
    This function handles the preparation and publishing of email notification data based on
    the ELT job execution status (success/failure). It retrieves job metadata, error messages,
    and log links, then publishes this information to a Pub/Sub topic for email notification.
    The function processes three main scenarios:
    1. Job failure - Includes error details and log links
    2. Job success - Includes success metadata
    3. Start dag error - Handles errors during DAG initialization
    Args:
        None
    Returns:
        None
    Raises:
        Exception: If there are any errors during metadata preparation or publishing
    Note:
        - Uses XCom to pull data from previous tasks
        - Logs execution details to Cloud Logging
        - Formats job metadata including start/end times
        - Adds log links to error messages for better traceability
    """

    ELT_ID=None
    job_id=None
    logger_info(logger, log_name, ELT_ID, job_id, 'begin: prepare_mail_task',default_timezone)
    context = get_current_context()
    ti = context['ti']
    
    log_resource_usage('prepare_mail_task', logger, log_name, ELT_ID, job_id, default_timezone)
    
    prepare_mail_task_error = ''
    elt_job_type = context['dag'].dag_id
    elt_job_type = elt_job_type.replace('_',' ')
    try:
        ELT_ID = ti.xcom_pull(task_ids='start_dag_fn', key='ELT_ID')
        ELT_JOB_ID = ti.xcom_pull(task_ids='initialize_job_run', key='ELT_JOB_ID')
        ELT_JOB_ID = str(ELT_JOB_ID) if ELT_JOB_ID is not None else str(None)
        config = ti.xcom_pull(task_ids='start_dag_fn', key='config')  #getting config from read config task

        start_dag_error = ti.xcom_pull(task_ids='start_dag_fn', key='task_name')
        logger_info(logger, log_name, ELT_ID, job_id, f'start_dag_error: {start_dag_error}',default_timezone)
        initialize_job_run_error = ti.xcom_pull(task_ids='initialize_job_run', key='task_name')
        logger_info(logger, log_name, ELT_ID, job_id, f'initialize_job_run_error: {initialize_job_run_error}',default_timezone)

        success_message = ti.xcom_pull(task_ids='mark_job_success', key='success_message')
        logger_info(logger, log_name, ELT_ID, job_id, f'success_message: {success_message}',default_timezone)
        failed_message = ti.xcom_pull(task_ids='mark_job_failure' , key='failed_message')
        logger_info(logger, log_name, ELT_ID, job_id, f'failed_message: {failed_message}',default_timezone)

        if failed_message:
            metadata = failed_message

            start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time')
            logger_info(logger, log_name, ELT_ID, job_id, f'failed_message start_time: {start_time}',default_timezone)
            end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
            metadata['ELT_JOB_START_TIME'] = start_time
            metadata['ELT_JOB_END_TIME'] = end_time
            metadata["ELT_JOB_TYPE"]=metadata["ELT_JOB_TYPE"].replace('_',' ')

            # get the log links for given elt_id 
            logs_link = get_log_link(start_time)
    
            # adding log links in the failed message
            metadata["ELT_JOB_ERROR_MESSAGE"]=metadata["ELT_JOB_ERROR_MESSAGE"] + logs_link
            logger_info(logger, log_name, ELT_ID, job_id, f'metadata: {metadata}',default_timezone)

        elif success_message:
            metadata = success_message
            start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time')
            logger_info(logger, log_name, ELT_ID, job_id, f'success_message start_time: {start_time}',default_timezone)
            end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
            metadata['ELT_JOB_START_TIME'] = start_time
            metadata['ELT_JOB_END_TIME'] = end_time
            metadata["ELT_JOB_TYPE"]=metadata["ELT_JOB_TYPE"].replace('_',' ')
            logger_info(logger, log_name, ELT_ID, job_id, f'metadata: {metadata}',default_timezone)

        elif start_dag_error:
            end_time = get_current_timestamp(default_timezone)  # Convert to ISO format for BigQuery
            start_time = ti.xcom_pull(task_ids='start_dag_fn', key='start_time')
            task_name  = ti.xcom_pull(task_ids=start_dag_error, key='task_name')
            task_error = ti.xcom_pull(task_ids=start_dag_error, key='task_error')
            logger_info(logger, log_name, ELT_ID, job_id, f'start_dag_error: {task_error}',default_timezone)
            logger_info(logger, log_name, ELT_ID, job_id, f'start_time: {start_time}',default_timezone)
            metadata = {
                "ELT_JOB_ID": ELT_JOB_ID,
                "ELT_ID": ELT_ID,
                "ELT_JOB_TYPE": elt_job_type,
                "ELT_JOB_NAME": task_name,
                "ELT_JOB_START_TIME": start_time,
                "ELT_JOB_END_TIME": end_time,
                "ELT_JOB_STATUS": "failure",
                "ELT_JOB_ERROR_MESSAGE": task_error,
                "ELT_INPUT_RECORDS": None,
                "ELT_OUTPUT_RECORDS": None,
            }

            # get the log links for given elt_id 
            logs_link = get_log_link(start_time)
    
            # adding log links in the failed message
            metadata["ELT_JOB_ERROR_MESSAGE"]=metadata["ELT_JOB_ERROR_MESSAGE"] + logs_link
            logger_info(logger, log_name, ELT_ID, job_id, f'metadata: {metadata}',default_timezone)

        logger_info(logger, log_name, ELT_ID, job_id, f"Metadata: {metadata}",default_timezone)

        publish_to_pubsub(metadata, context, 'prepare_mail_task_wrapped', 'pubsub_email', config)

    except Exception as e:
        if not prepare_mail_task_error:
            prepare_mail_task_error = str(e)
        logger_error(logger, log_name, ELT_ID, job_id, prepare_mail_task_error,default_timezone)
        ti.xcom_push(key='task_name', value='prepare_mail_task')
        ti.xcom_push(key='task_error', value=str(prepare_mail_task_error))
        raise
    logger_info(logger, log_name, ELT_ID, job_id, 'end: prepare_mail_task',default_timezone)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2016, 1, 1, tz=default_timezone),  # Set the start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id=dag_id,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
        params={'config_file':Param(
                type="string",
                title="config path",
                description="Enter a config path with json"
                )}
) as dag:

    # Override trigger rules correctly on the decorated @task functions
    mark_success = mark_job_success.override(trigger_rule=TriggerRule.ALL_SUCCESS)
    mark_failure = mark_job_failure.override(trigger_rule=TriggerRule.ONE_FAILED)
    end_dag_override = end_dag.override(trigger_rule=TriggerRule.ALL_DONE)
    prepare_mail_override = prepare_mail_task.override(trigger_rule=TriggerRule.ALL_DONE)

    # Instantiate tasks
    start_task = start_dag_fn()
    initialize_job_task = initialize_job_run()
    api_to_gcs_task = api_to_gcs()
    gcs_to_bq_task = gcs_to_bigquery()
    store_procedure_task = store_procedure()
    mark_success_task = mark_success()
    mark_failure_task = mark_failure()
    end_task = end_dag_override()
    prepare_mail_task = prepare_mail_override()

    # Set task dependencies
    start_task >> initialize_job_task >> api_to_gcs_task >> gcs_to_bq_task >> store_procedure_task
    [start_task, initialize_job_task, api_to_gcs_task, gcs_to_bq_task, store_procedure_task ] >> mark_failure_task
    [start_task, initialize_job_task, api_to_gcs_task, gcs_to_bq_task, store_procedure_task ] >> mark_success_task
    [mark_success_task, mark_failure_task] >> end_task >> prepare_mail_task

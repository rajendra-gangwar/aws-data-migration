"""
Python Code to take a backup of 
mysql schema for specific instance
and POST it to AWS S3 bucket
"""

import boto3 
import json
import logging
import urllib
import subprocess
import os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create the S3 client globally
s3_client = boto3.client('s3')

# list of database to trigger restore lambda
schema_backup_list = []

# slack creds
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL') # Enter the Slack channel to send a message to
HOOK_URL = os.getenv('HOOK_URL')

def get_secret(secret_name, region_name):
    """
    Fetch the secret from AWS Secrets Manager.
    
    :param secret_name: The name of the secret in AWS Secrets Manager.
    :param region_name: The AWS region where the secret is stored.
    :return: The secret as a dictionary.
    """
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def delete_lines(dump_file):

    if os.path.exists(dump_file):
        delete_cmds = [
            "sed -i '/^SET @@SESSION.SQL_LOG_BIN/d' {}".format(dump_file),
            "sed -i '/^SET @@GLOBAL.GTID_PURGED/d' {}".format(dump_file)
        ]

        for cmd in delete_cmds:
            subprocess.check_call(cmd, shell=True)
        logger.info(f"sed command executed on {dump_file}")
    else:
        logging.error(f"dump_file doesn't exists to upload {dump_file}")

def send_slack_notification(message, color="grey"):
    """
    Send the notification to slack channel
    """
    COLORS = {
        'red': '#ff0000',
        'green': '#36a64f',
        'blue': '#0072bb',
        'yellow': '#ffff00',
        'grey': '#cccccc'
    }
    color_code = COLORS.get(color.lower())
    attachments = [
        {
            "fallback": message,
            "color": color_code,
            "text": message,
        }
    ]

    slack_message = {
        'channel': SLACK_CHANNEL,
        'attachments': attachments
    }

    logger.info(f"Sending Slack notification: {message}")
    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'), headers={'Content-Type': 'application/json'})
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['channel'])
    except HTTPError as e:
        logger.error(f"Request failed: {e.code} {e.reason}")
    except URLError as e:
        logger.error(f"Server connection failed: {e.reason}")

def schema_backup(database):
    """
    Backup of instance schema
    """
    global schema_backup_list
    source_host = os.getenv('SOURCE_HOST')
    source_user = os.getenv('SOURCE_USER')
    source_password = os.getenv('SOURCE_PASSWORD')

    if not source_host or not source_user or not source_password:
        raise ValueError("Source database credentials are not set in environment variables")

    dump_file = f"/tmp/{database}.sql"
    # Run mysqldump command to export the source database
    logger.info(f"/opt/bin/mysqldump --skip-set-charset -h {source_host} -u {source_user} --password='******' --no-data --skip-add-drop-table --databases {database} > {dump_file} ")
    try:
        dump_cmd = f"/opt/bin/mysqldump --skip-set-charset -h {source_host} -u {source_user} --password='{source_password}' --no-data --skip-add-drop-table --databases {database} > {dump_file} "
        subprocess.check_call(dump_cmd, shell=True)

        # delete lines requires auper privileges
        delete_lines(dump_file)
        schema_backup_list.append(database)
    except Exception as e:
        logging.error(f"Error occurred in mysqldump for {database}: {str(e)}")
        send_slack_notification(f'<schema backup lambda>: Error occurred for schema backup of database {database}','red')
        return None
    return database

def schema_copy_to_s3(s3_bucket, database):
    """
    Uploads the specified dump file to an S3 bucket.

    The S3 bucket name and region are read from the environment variables S3_BUCKET_NAME and S3_REGION.
    """
    global schema_backup_list

    dump_file = f"/tmp/{database}.sql"
    status = 0
    try:
        # Upload the file to S3
        s3_client.upload_file(dump_file, s3_bucket, os.path.basename(dump_file))
        logging.info(f"Successfully uploaded {dump_file} to s3://{s3_bucket}/{os.path.basename(dump_file)}")
        status = 1
    except FileNotFoundError:
        logging.error(f"Error: The file {dump_file} was not found")
    except NoCredentialsError:
        logging.error("Error: AWS credentials not available")
    except PartialCredentialsError:
        logging.error("Error: Incomplete AWS credentials")
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
    
    if status == 0:
        send_slack_notification(f"ERROR: S3 upload Failed: {database}.sql to s3://{s3_bucket}", 'red')
        schema_backup_list.remove(database)
    else:
        send_slack_notification(f"INFO: S3 upload Successful: {database}.sql to s3://{s3_bucket}", 'green')
    

def trigger_lambda(function, region, payload):
    """
    Trigger another lambda function
    """
    try:

        # Make connection to AWS lambda
        lambda_client = boto3.client('lambda', region_name=region)

        # Invoke the Lambda function
        trigger_response = lambda_client.invoke(
            FunctionName = function,
            InvocationType = 'RequestResponse',  # Use 'Event' for asynchronous invocation
            Payload = payload
        )
        
        logger.info(f"INFO: Invoked Lambda function {function} with payload: {payload}  --> {trigger_response}")

    except Exception as e:
        logger.error(f"ERROR: Invoking Lambda function: {e}")
        
    return {
            'StatusCode': trigger_response.get('StatusCode')
        }

def lambda_handler(event, context):

    send_slack_notification(f"-------------------- RDS Database Schema Backup -------------------", "yellow")
    logger.info(f"Payload: {event}")
    # Get database credentails to connect
    source_secret_name = event["schema"]["export_db"]["db_secret"]
    secret_region = event["schema"]["export_db"]["region"]

    source_db_secret = get_secret(source_secret_name, secret_region)

    # Setting Environment variable for database admin credentails
    os.environ['SOURCE_HOST'] = source_db_secret['hostname']
    os.environ['SOURCE_USER'] = source_db_secret['username']
    os.environ['SOURCE_PASSWORD'] = source_db_secret['password']

    # Get the instance details
    databases = event['instances']
    s3_bucket = event["schema"]["storage"]["bucket"]

    for db in databases:
        instance = schema_backup(db)
        if instance:
            schema_copy_to_s3(s3_bucket, instance)

    # Constructing payload for import lambda

    import_payload = json.dumps({
        "databases": schema_backup_list[:],
        "schema": {
            "storage": event["schema"]["storage"],
            "import_db": event["schema"]["import_db"]
            }
        })

    # Trigger lambda for import the schema in target database

    if event["schema"]["import_db"]["action"] == "import":
        trigger_response = trigger_lambda(event["schema"]["import_db"]["function"], event["schema"]["import_db"]["region"], import_payload)

        if trigger_response.get('StatusCode') == 200:
            logger.info(f"INFO: import lambda function triggered successfully: {trigger_response}")

        else:
            logger.error(f"ERROR: import lambda function failed to trigger: {trigger_response}")
            send_slack_notification(f"ERROR: import lambda function failed to trigger - check logs for more detail", "red")
    
    else:
            logger.info(f"INFO: Import lambda function skipped as action is not 'import'")
            send_slack_notification(f"INFO: Import lambda function skipped as action is set to {event["schema"]["import_db"]["action"]}", "yellow")
    
"""
Lambda Function to import 
RDS Database schema without data
"""
import json
import subprocess
import boto3
import logging
import os
import urllib
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import pymysql

# slack creds
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL') # Enter the Slack channel to send a message to
HOOK_URL = os.getenv('HOOK_URL')

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

db_import_success = []

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
    # slack_message = {'channel': SLACK_CHANNEL, 'text': message}
    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'), headers={'Content-Type': 'application/json'})
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['channel'])
    except HTTPError as e:
        logger.error(f"Request failed: {e.code} {e.reason}")
    except URLError as e:
        logger.error(f"Server connection failed: {e.reason}")


def schema_restore(database):

    """
    Export instance schema
    """
    
    global db_import_success
    target_host = os.getenv('TARGET_HOST')
    target_user = os.getenv('TARGET_USER')
    target_password = os.getenv('TARGET_PASSWORD')

    if not target_host or not target_user or not target_password:
        raise ValueError("Source database credentials are not set in environment variables")
    
    s3_bucket = os.getenv('S3_SCHEMA_BUCKET')
    s3_key = f"{database}.sql"
    download_path = f"/tmp/{s3_key}"

    try:
        s3_client.download_file(s3_bucket, s3_key, download_path)

        import_cmd = f"/opt/bin/mysql  -h {target_host} -u {target_user} --password='{target_password}' < {download_path}"
        subprocess.check_call(import_cmd, shell=True)
        db_import_success.append(database)
        logger.info(f"SUCCESS: Database imported to target db {database}")
        send_slack_notification(f'SUCCESS: Database imported to target db {database}', "green")

    except subprocess.CalledProcessError as e:
        logger.info(f"Error: During mysql import for {database} ==> {str(e)}")
        send_slack_notification(f"Error: During mysql import for {database}", "red")
        

    except s3_client.exceptions.NoCredentialsError:
        logger.info(f"ERROR: Credentials not available for AWS: {str(e)}")
        send_slack_notification(f"ERROR: Credentials not available for AWS: {str(e)}", "red")

    except Exception as e:
        logger.info(f"ERROR: An error occurred in mysql import: {str(e)}")
        send_slack_notification(f"ERROR: An error occurred in mysql import: {str(e)}","red")
        


def user_add(data, databases, region):
    
    secret_name_prefix = data['prefix']
    
    try:
        # Connect to the RDS instance
        connection = pymysql.connect(
            host        =   os.getenv('TARGET_HOST'),
            port        =   3306,
            user        =   os.getenv('TARGET_USER'),
            password    =   os.getenv('TARGET_PASSWORD'),
            charset     =   'utf8mb4',
            cursorclass =   pymysql.cursors.DictCursor
        )
    except Exception as e:
        logger.error(f"Error retrieving RDS connection details or establishing connection: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error retrieving RDS connection details or establishing connection.')
        }

    try:

        secret_client = boto3.client('secretsmanager', region_name = region)
        # Iterate over each instance provided in the event
        for instance in databases:
            secret_name = f"{secret_name_prefix}{instance}"
            
            try:
                # Retrieve the secret for the specific instance
                get_secret_value_response = secret_client.get_secret_value(SecretId=secret_name)
                secret = json.loads(get_secret_value_response['SecretString'])
                username = secret['username']
                password = secret['password']
                
                with connection.cursor() as cursor:
                    # Create a new user
                    cursor.execute(f"CREATE USER `{username}`@'%' IDENTIFIED BY '{password}';")
                    
                    # Grant full access to the instance schema
                    cursor.execute(f"GRANT ALL PRIVILEGES ON `{instance}`.* TO '{username}'@'%';")
                    
                    # Commit the changes
                    connection.commit()
                    send_slack_notification(f"User --> {username} Created for --> {instance} ", "green")
                    
            except Exception as e:
                logger.error(f"Error managing user for instance {instance}: {e}")
                send_slack_notification(f"ERROR occured creating instance user for {instance}", "red")
                continue

    finally:
        connection.close()

def trigger_lambda(function, region, payload):
    """
    Trigger another lambda function
    """
    
    # increasing tmeout for boto3 connection
    config = Config(
        read_timeout=120,
        retries={
            'max_attempts': 0,
            'mode': 'standard'
        }
    )
    
    try:

        # Make connection to AWS lambda
        lambda_client = boto3.client('lambda', region_name=region, config=config)

        # Invoke the Lambda function
        trigger_response = lambda_client.invoke(
            FunctionName = function,
            InvocationType = 'RequestResponse',
            Payload = payload
        )
        
        logger.info(f"INFO: Invoked Lambda function {function} with payload: {payload}  --> {trigger_response}")
        return {
            'StatusCode': 200
        }

    except lambda_client.exceptions.ResourceNotFoundException:
            logger.error(f"ERROR: Lambda function not found ")
            return {
            'StatusCode': 404
        }
        
    except Exception as e:
        logger.error(f"ERROR: Invoking Lambda function: {e}")
        return {
            'StatusCode': 500
        }
        


def lambda_handler(event, context):
    """
    Event and trigger will be executed from backup lambda
    Bucket and databases details will be included in event body
    """

    logger.info(f"Payload: {event}")
    # Fetch environment variables
    os.environ['S3_SCHEMA_BUCKET'] = event['schema']['storage']['bucket']
    databases = event['databases']

    target_secret_name = event["schema"]['import_db']['db_secret']
    region = event["schema"]['import_db']['region']

    # Setting Environment variable for database admin credentails
    target_db_secret = get_secret(target_secret_name, region)
    os.environ['TARGET_HOST'] = target_db_secret['hostname']
    os.environ['TARGET_USER'] = target_db_secret['username']
    os.environ['TARGET_PASSWORD'] = target_db_secret['password']

    for db in databases:
        schema_restore(db)

    # calling user function
    
    logger.info(f"User creation executing on list : {db_import_success}")
    user_add(event['schema']['import_db'], db_import_success, region)

    # Trigger for DMS function to start the sync

    dms_event =  event['schema']['import_db']['dms']
    dms_payload = json.dumps({
        "databases": db_import_success[:],
        "dms": dms_event
            }
        )
    
    logger.info(f"dms event: {dms_event}")
    logger.info(f"dms payload: {dms_payload}")
    
    if dms_event['action'] == 'sync' and event['schema']['import_db']['action'] == 'import':
        dms_response = trigger_lambda(dms_event['function'], dms_event['region'], dms_payload)
        
        if dms_response.get('StatusCode') == 200:
            logger.info(f"INFO: dms replication lambda triggered")
            
        else:
            logger.error(f"dms replication lambda trigger failed: {dms_response}")
            send_slack_notification(f"dms replication lambda failed to trigger", "red")
        
    return {
        "message": "Import lambda execution completed",
        "status": 200
    }

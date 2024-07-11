import boto3 
import json
import logging
import os
import time
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# slack creds
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL') # Enter the Slack channel to send a message to
HOOK_URL = os.getenv('HOOK_URL')


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


def generate_rules(schemas):
    rules = []
    for i, schema in enumerate(schemas):
        rule_id = f"{i:09d}"
        rule = {
            "rule-type": "selection",
            "rule-id": rule_id,
            "rule-name": rule_id,
            "object-locator": {
                "schema-name": schema,
                "table-name": "%"
            },
            "rule-action": "include",
            "filters": []
        }
        rules.append(rule)
    return json.dumps({"rules": rules})

# Function to check the replication task status
def get_task_status(task_arn, region):
    
    dms_client = boto3.client('dms', region_name=region)
    response = dms_client.describe_replication_tasks(
        Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
    )
    tasks = response['ReplicationTasks']
    if not tasks:
        raise ValueError(f"No replication task found with ARN: {task_arn}")
    return tasks[0]['Status']
    
    
def lambda_handler(event, context):
    
    send_slack_notification("-------- DMS Task trigger Execution Started ---------", "yellow")
    logger.info(f"Payload: {event}")

    schemas = event['databases']

    dms_task_arn = event['dms']['task_arn']
    region = event['dms']['region']

    table_mapping = generate_rules(schemas)

    dms_client = boto3.client('dms', region_name=region)

    try:
        dms_response = dms_client.modify_replication_task(
                ReplicationTaskArn  =   dms_task_arn,
                MigrationType       =   'full-load-and-cdc',
                TableMappings       =   table_mapping
            )
    except dms_client.exceptions.InvalidResourceStateFault as e:
        logger.error(f"task is not in desired state to modify: {e}")
        send_slack_notification(f"ERROR: dms function Modification failed due to current state: {get_task_status(dms_task_arn, region)}\ndms task: {dms_task_arn}", "red")
        return {
            "message": "dms task failed",
            "status": 500
        }
    except Exception as e:
        logger.error(f"unexpected error occured in dms task modification {dms_task_arn} {e}")
        send_slack_notification(f"ERROR: unexpected error occured\n{dms_task_arn}", "red")
        return {
            "message": "unexpected error",
            "status": 500
        }            
    

    if dms_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info(f"INFO: dms task modified successfully: {dms_task_arn}")
        
        timeout = 180
        start_time = time.time()
        while True:
            status = get_task_status(dms_task_arn, region)
            print(f"Current task status: {status}")
            if status in ['ready', 'stopped']:
                logger.info("Modification completed. Task is ready to be started.")
                break
            elif status in ['modifying']:
                logger.info("Modification in progress. Waiting...")
            else:
                logger.error(f"Unexpected task status: {status}")
                raise Exception(f"Unexpected task status: {status}")
            time.sleep(30)
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                logger.error(f"Timeout: Task modification did not complete within {timeout} seconds.")
                raise TimeoutError("Timeout: Task modification did not complete within 180 seconds.")
        
        
        start_response = dms_client.start_replication_task(
                ReplicationTaskArn=dms_task_arn,
                StartReplicationTaskType='reload-target'
            )
            
        logger.info(f"DMS Task {dms_task_arn} started: {start_response}")
        
        if start_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"INFO: dms function started for full load with cdc: {dms_task_arn}")
            send_slack_notification(f"INFO: dms function started for full load with cdc: {dms_task_arn}", "yellow")
            
        else:
            logger.error(f"dms function failed to start: {dms_task_arn}")
            send_slack_notification(f"ERROR: dms function failed to start : {dms_task_arn}", "red")
            
        
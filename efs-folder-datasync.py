import boto3
import json
import os
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# slack creds
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL') # Enter the Slack channel to send a message to
HOOK_URL = os.getenv('HOOK_URL')

def update_datasync_task(instances, data):
    """
    Updating datasync task for including 
    EFS directory (Assuming directory are in root 
    folder as per the location configuration)
    Datasync Task required to pre-configurred 
    """

    # Create a DataSync client
    ds_client = boto3.client('datasync', region_name=data['region'])

    # Convert the list of includes to the required format
    include_instance = '|'.join(f'/{item}' for item in instances)

    try:
        # Update the DataSync task with the new options
        response = ds_client.start_task_execution(
            TaskArn=data['task_arn'],
            Includes=[
                        {
                            'FilterType': 'SIMPLE_PATTERN',
                            'Value': include_instance
                        }
                    ],
            OverrideOptions={
                'PreserveDeletedFiles': 'PRESERVE',
                'VerifyMode': 'ONLY_FILES_TRANSFERRED',
                'OverwriteMode': 'ALWAYS',
                'TransferMode': 'CHANGED'
            }
        )
        logger.info(f"EFS task executed with code {response['ResponseMetadata']['HTTPStatusCode']}")
        send_slack_notification("===== EFS Lambda Triggered for instance folder Migration =====","yellow")
        return response['ResponseMetadata']['HTTPStatusCode']

    except Exception as e:
        logger.info(f"EFS task trigger failed with exception: {str(e)}")
        send_slack_notification("ERROR: EFS Lambda Triggered failed", "red")

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


def lambda_handler(event, context):

    logger.info(f"Payload: {event}")
    # Create a DataSync client
    if event.get('efs', {}).get('action') == 'sync':
        update_datasync_task(event['instances'], event['efs'])
    else:
        send_slack_notification("INFO: skipping efs trigger action is not set to 'sync' or doesn't exists for efs", "yellow")

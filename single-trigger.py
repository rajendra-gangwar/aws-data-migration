import logging
import os
import boto3
import json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HOOK_URL = os.getenv('HOOK_URL')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')


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


def load_json(key, input_bucket):
    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=input_bucket, Key=key)
        config = json.loads(response['Body'].read())
        logger.debug(config)
    except Exception as e:
        logger.error(e)
        logger.error('Error getting object {} from bucket {}. '
            'Make sure they exist and your bucket is in the '
            'same region as this function.'.format(key, input_bucket))
        raise e
    return config


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

    logger.info(f"========== {context.function_name} Execution Initiated =========")
    logger.info(f"Payload: {event}")
    # Get the object from the event and show its content type
    input_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']


    payload = load_json(key, input_bucket)


    if payload['efs']['action'] == 'sync':
        payload_data = json.dumps({"instances": payload["instances"], "efs": payload['efs']})
        response = trigger_lambda(payload['efs']['function'], payload['efs']['region'], payload_data)
        if response.get("StatusCode") == 200:
            logger.info(f"==> {context.function_name} triggered Successfully for EFS")
        else:
            logger.error(f"----------------- {context.function_name} lambda trigger failed with status code {response['StatusCode']} -------------")
            send_slack_notification(f'ERROR: EFS lambda failed to trigger with status code {response["StatusCode"]}', "red")


    # if payload['dms']["action"] == 'sync':
    #     payload_data = {"instances": payload["instances"], "dms": payload['dms']}
    #     response = trigger_lambda(payload_data)
    #     if response["StatusCode"] == 200:
    #         logger.info(f"==> {context.function_name} triggered Successfully for DMS")
    #     else:
    #         logger.error(f"----------------- {context.function_name} DMS lambda trigger failed with status code {response['StatusCode']} -------------")
    #         send_slack_notification(f'ERROR: DMS lambda failed to trigger with status code {response["StatusCode"]}', "red")


    if payload['schema']['export_db']['action'] == 'export':
        payload_data = json.dumps({"instances": payload["instances"], "schema": payload["schema"]})
        response = trigger_lambda(payload["schema"]["export_db"]["function"], payload["schema"]["export_db"]["region"] ,payload_data)
        logger.info(f"export schema response ==> {response}")
        if response.get("StatusCode") == 200:
            logger.info(f"==> {context.function_name} triggered Successfully for Schema export")
        else:
            logger.error(f"----------------- {context.function_name} lambda trigger failed with status code {response['StatusCode']} -------------")
            send_slack_notification(f'ERROR: Schema Export lambda failed to trigger with status code {response["StatusCode"]}', "red")

    return {
        "message": "Trigger lambda execution completed",
        "status": 200
    }
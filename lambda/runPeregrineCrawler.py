import json
import urllib.parse
import boto3
from botocore.exceptions import ClientError

print('Loading function')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        glue_response = glue_client.start_crawler(Name='peregrineCrawler')
        return glue_response
    except ClientError as e:
        raise Exception("boto3 client error in start_a_crawler: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in start_a_crawler: " + e.__str__())
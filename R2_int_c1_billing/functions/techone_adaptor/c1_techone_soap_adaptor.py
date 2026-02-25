import logging
import json
import os
import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger("Billing Queue Consumer")
logger.setLevel(logging.INFO)


# Get Landmark SFTP details from Secret Manager
def get_techone_secret():
    """
    Function to get Techone Secreat details
    """
    logger.info("Entering get_techone_secret()")

    secret_name = os.environ["TECHONE_SECRET_NAME"]

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response["SecretString"])

    secret_dict = {
        "client_id": secret["client_id"],
        "client_secret": secret["client_secret"],
    }

    logger.info("Entering get_techone_secret()")

    return secret_dict


def lambda_handler(event, context):
    """
    Function to Invoke Techone API
    """
    logger.info("Entering lambda_handler()")

    techone_cred_dict = get_techone_secret()

    ssm_client = boto3.client("ssm")
    access_token_url = (
        ssm_client.get_parameter(Name=os.environ["TECHONE_API_ACCESS_TOKEN_URL"])
        .get("Parameter")
        .get("Value")
    )
    client_id = techone_cred_dict["client_id"]
    client_secret = techone_cred_dict["client_secret"]
    billing_soap_action_url = (
        ssm_client.get_parameter(Name=os.environ["TECHONE_BILLING_SOAP_ACTION_URL"])
        .get("Parameter")
        .get("Value")
    )

    auth_payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    payload = event["content"]
    logger.info(payload)
    soap_url = payload["soap_url"]
    content = payload["content"]
    content = content.strip()
    logger.info(content)

    token_response = requests.post(access_token_url, data=auth_payload)
    if token_response.status_code == 200:
        token = token_response.json()
        access_token = token["access_token"]

        techone_response = requests.post(
            soap_url,
            headers={
                "Accept": "text/xml",
                "Content-Type": "text/xml",
                "Authorization": access_token,
                "SOAPAction": billing_soap_action_url,
            },
            data=content,
        )
        logger.info(techone_response)
        techone_response_text = techone_response.text
        logger.info(techone_response_text)
        logger.info(techone_response.content)
        logger.info(type(techone_response))

    else:
        logger.error("Error in C1 Token Generation")
        logger.error(token_response.status_code)
        techone_response_text = "Error in Fetching Token"

    logger.info("Existing lambda_handler")

    return techone_response_text

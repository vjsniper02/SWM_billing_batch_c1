import json
import boto3
import paramiko
from io import BytesIO
from stat import S_ISDIR, S_ISREG
from botocore.exceptions import ClientError
from io import StringIO
import logging
import os
import io

logger = logging.getLogger("d6_landmark_sftp")
logger.setLevel(logging.INFO)


# Get Landmark SFTP details from Secret Manager
def get_secret(secret_name):
    logger.info(f"Start get_secret()")

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
        "ftp_url": secret["ftp_url"],
        "user_id": secret["user_id"],
        "password": secret["password"],
        "key_value": secret["key_value"],
    }

    logger.info(f"End get_landmark_secret()")

    return secret_dict


def connect_to_sftp(hostname, username, ssh_key):
    transport = paramiko.Transport((hostname, 22))
    transport.connect(username=username, pkey=ssh_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


def push_file_to_ga(bucket_name, file_name, secret_name, ftp_path):
    logger.info("Entry push_file_to_ga")
    ssm_client = boto3.client("ssm")

    ftp_dict = get_secret(secret_name)

    ftp_url = ftp_dict["ftp_url"]
    user_id = ftp_dict["user_id"]
    key_value = ftp_dict["key_value"]

    private_key_file = StringIO()
    private_key_file.write(key_value)
    private_key_file.seek(0)

    ssh_key = paramiko.RSAKey.from_private_key(private_key_file)

    sftp_path = ssm_client.get_parameter(Name=ftp_path).get("Parameter").get("Value")
    logger.info(file_name)
    file_name_new = file_name.split("/")[1]
    logger.info(file_name_new)
    logger.info(sftp_path)
    sftp_path = sftp_path + "/" + file_name_new
    logger.info(sftp_path)

    try:
        # Download the file from S3
        s3 = boto3.client("s3")
        s3_response_object = s3.get_object(Bucket=bucket_name, Key=file_name)
        s3_object_body = s3_response_object["Body"].read()
        s3_file_content = BytesIO(s3_object_body)
        ftp_client = connect_to_sftp(ftp_url, user_id, ssh_key)
        logger.info("Connected to GA STP")
        ftp_client.putfo(s3_file_content, sftp_path)
        ftp_client.close()
    except IOError as e:
        logger.exception(f"Error copying file to GA IO Exception- {str(e)}")
        logger.error("Error copying file to GA")
        ftp_client.close()

    except paramiko.SSHException as e1:
        logger.exception(f"Error copying file to GA SSH Exception - {str(e1)}")
        logger.error("Connection Error")

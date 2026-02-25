import json
from io import StringIO
import logging
import os
import csv
import paramiko
import datetime
import pytz
import boto3
import time
from ga_sftp import push_file_to_ga

logger = logging.getLogger("d2_landmark_sftp")
logger.setLevel(logging.INFO)

# Environment variables
s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")

LANDMARK_SFTP_PATH = os.environ["LANDMARK_SFTP_PATH"]
SEIL_S3_BUCKET = os.environ["SEIL_S3_BUCKET"]
LANDMARK_SECRET_NAME = os.environ["LANDMARK_SFTP_SECRET_NAME"]
LANDMARK_SECRET_NAME = os.environ["LANDMARK_SFTP_SECRET_NAME"]
LANDMARK_DELETE_SECRET_NAME = os.environ["LANDMARK_SFTP_SECRET_NAME_DELETE"]
GA_SFTP_SECRET_NAME = os.environ["GA_SFTP_SECRET_NAME"]
GA_FTP_PATH = os.environ["GA_FTP_PATH"]
sqs_queue_url = os.environ[
    "SQS_QUEUE_URL"
]  # Add this line with your actual SQS queue URL


def get_secret_credentials(secret_name):
    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response["SecretString"])

        return {
            "host": secret["ftp_url"],
            "username": secret["user_id"],
            "password": secret["password"],
            "key_value": secret.get("key_value", None),
        }
    except Exception as e:
        logger.error(f"Error retrieving SFTP credentials from Secret Manager: {str(e)}")
        return None


s3 = boto3.client("s3")
sqs = boto3.client("sqs")


def parse_csv(csv_data):
    # Decode binary data to string
    csv_data_str = csv_data.decode("charmap")

    # Parse CSV data and return list of rows
    rows = []
    csv_reader = csv.reader(StringIO(csv_data_str))
    for row in csv_reader:
        rows.append(row)
    return rows


def connect_to_sftp(hostname, username, password, ssh_key):
    """
    Function to fetch secret value for file transfer1
    """
    logger.info("Entering connect_to_sftp()")
    transport = paramiko.Transport((hostname, 22))
    transport.connect(username=username, password=password, pkey=ssh_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logger.info("Exiting connect_to_sftp()")
    return sftp


def lambda_handler(event, context):
    try:
        try:
            allowed_schedule_range = os.environ["ALLOWED_SCHEDULE_RANGE"]
            ssm = boto3.client("ssm", region_name="ap-southeast-2")  # Sydney region
            parameter = ssm.get_parameter(
                Name=allowed_schedule_range, WithDecryption=True
            )
            allowed_range_str = parameter["Parameter"]["Value"]
            start_day, end_day = map(int, allowed_range_str.split("-"))
        except Exception as e:
            logger.info(f"Error retrieving allowed schedule range from SSM: {e}")
            return

        sydney_tz = pytz.timezone("Australia/Sydney")
        now = datetime.datetime.now(sydney_tz)
        current_day = now.day

        if start_day <= current_day <= end_day:
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                # Retrieve Landmark SFTP credentials from Secret Manager
                landmark_credentials = get_secret_credentials(LANDMARK_SECRET_NAME)

                delete_ftp_dict = get_secret_credentials(LANDMARK_DELETE_SECRET_NAME)
                delete_private_key_file = StringIO()
                delete_private_key_file.write(delete_ftp_dict["key_value"])
                delete_private_key_file.seek(0)
                delete_ssh_key = paramiko.RSAKey.from_private_key(
                    delete_private_key_file
                )

                try:
                    ssh_client.connect(
                        hostname=landmark_credentials["host"],
                        username=landmark_credentials["username"],
                        password=landmark_credentials["password"],
                        port=22,
                        allow_agent=False,
                        pkey=paramiko.RSAKey(
                            file_obj=StringIO(landmark_credentials["key_value"])
                        ),
                    )

                    with ssh_client.open_sftp() as sftp:
                        sftp_path = (
                            ssm_client.get_parameter(Name=LANDMARK_SFTP_PATH)
                            .get("Parameter")
                            .get("Value")
                        )
                        sftp.chdir(sftp_path)
                        logger.info("Landmark SFTP Connection Established")

                        # List files in the SFTP directory
                        files_on_sftp = sftp.listdir()

                        delete_ftp_client = connect_to_sftp(
                            delete_ftp_dict["host"],
                            delete_ftp_dict["username"],
                            delete_ftp_dict["password"],
                            delete_ssh_key,
                        )

                        # Process each CSV file on the SFTP server
                        for csv_file_name in files_on_sftp:
                            if csv_file_name.lower().endswith(".csv"):
                                try:
                                    # Download CSV file
                                    csv_file_data = sftp.open(csv_file_name).read()

                                    # Upload original CSV to 'raw' folder in S3
                                    raw_csv_key = f"raw/{csv_file_name}"
                                    s3_client.put_object(
                                        Body=csv_file_data,
                                        Bucket=SEIL_S3_BUCKET,
                                        Key=raw_csv_key,
                                    )

                                    time.sleep(20)
                                    # Push File to GoAnywhere SFTP Server
                                    push_file_to_ga(
                                        SEIL_S3_BUCKET, raw_csv_key, GA_SFTP_SECRET_NAME, GA_FTP_PATH
                                    )
                                    
                                    logger.info(f"File downloaded {csv_file_name} ")

                                    # Parse CSV file
                                    csv_rows = parse_csv(csv_file_data)
                                    row1 = csv_rows[0]

                                    # Split CSV rows into chunks (5000 rows each) and process
                                    rows_per_chunk = 4999
                                    for idx, start_index in enumerate(
                                        range(0, len(csv_rows), rows_per_chunk)
                                    ):
                                        end_index = start_index + rows_per_chunk
                                        chunk_rows = csv_rows[start_index:end_index]

                                        # Convert chunk rows back to CSV data
                                        csv_chunk_data = StringIO()
                                        csv_writer = csv.writer(csv_chunk_data)
                                        if idx > 0:
                                            csv_writer.writerow(row1)
                                        csv_writer.writerows(chunk_rows)
                                        csv_chunk_data.seek(0)

                                        # Calculate record count range
                                        record_count_start = start_index
                                        record_count_end = min(
                                            end_index - 1, len(csv_rows) - 1
                                        )
                                        record_count_range = (
                                            f"{record_count_start}_{record_count_end}"
                                        )

                                        # Upload CSV chunk to S3 with record count range in the filename
                                        csv_key = f"queue/{csv_file_name}_{record_count_range}_{idx + 1}.csv"
                                        s3_client.put_object(
                                            Body=csv_chunk_data.read(),
                                            Bucket=SEIL_S3_BUCKET,
                                            Key=csv_key,
                                        )
                                        # Send CSV key to SQS
                                        sqs.send_message(
                                            QueueUrl=sqs_queue_url, MessageBody=csv_key
                                        )
                                    file_with_path = sftp_path + "/" + csv_file_name
                                    delete_file_path = file_with_path.replace(
                                        "/ftp.out", ""
                                    )

                                    logger.info(f"Deleting file: {delete_file_path}")
                                    delete_ftp_client.remove(delete_file_path)

                                except Exception as download_error:
                                    logger.error(
                                        f"Error processing CSV file '{csv_file_name}' from SFTP: {str(download_error)}"
                                    )
                        delete_ftp_client.close()

                except Exception as connection_error:
                    logger.error(
                        f"Error connecting to SFTP server: {str(connection_error)}"
                    )

            except Exception as e:
                logger.error(f"Unhandled exception: {str(e)}")
        else:
            logger.info("Scheduler Ignored")
    except Exception as e:
        logger.info(f"Error retrieving allowed schedule range from SSM: {e}")

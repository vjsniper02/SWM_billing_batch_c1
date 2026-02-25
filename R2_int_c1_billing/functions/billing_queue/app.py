import json
import boto3
from io import StringIO
import csv
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
sqs = boto3.client("sqs")

SEIL_S3_BUCKET = "your-s3-bucket"  # Replace with your actual S3 bucket name
SQS_QUEUE_URL = "your-sqs-queue-url"  # Replace with your actual SQS queue URL


def construct_soap_request(csv_rows):
    # Implement your logic to construct TechOne SOAP request from CSV data
    # Return the SOAP request as a string
    soap_request = """
   <?xml version="1.0" encoding="UTF-8"?> 
        <soapenv:Envelope 
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
        xmlns:ns1="http://TechnologyOneCorp.com/Public/Services"> 
        <soapenv:Header/> 
        <soapenv:Body> 
            <ns1:Warehouse_DoImport> 
                <ns1:request WarehouseName="SALESFORCE" WarehouseTableName="BILLINGDATA" ImportMode="InsertMode"> 
                    <ns1:Auth UserId="SRV_SFPROXY" Password="password" Config="SEVEN-TEST-CES direct" FunctionName="$E1.BI.WHT.DOIMP.WS"/> 

                    <ns1:Columns> 
                    <ns1:ColumnInfo Name="SEQNO"/> 
                    <ns1:ColumnInfo Name="GENERALLEDGERCODE"/> 
        . . .
                    <ns1:ColumnInfo Name="REVENUETYPE"/> 
                    <ns1:ColumnInfo Name="GOODSRECEIVED"/> 
                    </ns1:Columns> 
                    <ns1:Rows> 
                    <ns1:Row>1243,110-0608-00604,Seven,Standard,,,Reach,7 - Reach - Digital Only - 00000,,FALSE,JitJatJo  Launch Campaign  7Plus - June 2022,JITJATJO AUSTRALIA PTY LTD,JITJATJO AUSTRALIA PTY LTD,19999.98,19999.98,0,AUD,14858,,76 618 587 036,,JITJAT20,319.0992,319.0992,Australian GST,10,NetCost,Australian Tax Authority,19999.98,19999.98,3190.992,3190.992,1000,Fish,28034,JitJatJo  Launch Campaign  7Plus - June 2022 - May-2022,25/05/2022 0:00,23/06/2022 0:00,7Plus Video :15 | Demo &amp; Postcode Targeting,Prepay,476190,Prepay,na@seven.com.au,TypeA</ns1:Row> 
                    <ns1:Row>1244,110-0608-00604,Seven,Standard,,,Reach,7 - Reach - Digital Only - 00000,,FALSE,Fresh Markets Australia | VIC | 7Plus | May-June 2022,THE AUSTRALIAN CHAMBER OF FRUIT AND VEGETABLE INDUSTRIES LIMITED ,THE AUSTRALIAN CHAMBER OF FRUIT AND VEGETABLE INDUSTRIES LIMITED ,4500.01,4500.01,0,AUD,15010,,67 065 246 808,0,FREMAR30,145.6475,145.6475,Australian GST,10,NetCost,Australian Tax Authority,4500.01,4500.01,1456.475,1456.475,700,Salad,27946,Fresh Markets Australia | VIC | 7Plus | May-June 2022 - May-2022,22/05/2022 0:00,19/06/2022 0:00,7Plus | 30s | All Devices | GeoPC | Data,Prepay,78261,Prepay,bboardman@seven.com.au,TypeA</ns1:Row> 
                    </ns1:Rows> 
                </ns1:request> 
            </ns1:Warehouse_DoImport> 
        </soapenv:Body> 
        </soapenv:Envelope> 
    """
    return soap_request


def move_file(source_key, destination_folder):
    # Move the file to the specified folder
    try:
        copy_source = {"Bucket": SEIL_S3_BUCKET, "Key": source_key}
        destination_key = f'{destination_folder}/{source_key.split("/")[-1]}'
        s3_client.copy_object(
            CopySource=copy_source, Bucket=SEIL_S3_BUCKET, Key=destination_key
        )
        s3_client.delete_object(Bucket=SEIL_S3_BUCKET, Key=source_key)
    except ClientError as e:
        print(f"Error moving file {source_key} to {destination_folder}: {e}")


def handler(event, context):
    result = {"batchItemFailures": []}

    for msg in event["Records"]:
        try:
            # Retrieve CSV file from S3
            sqs_message = json.loads(msg["body"])
            csv_key = sqs_message["MessageBody"]
            csv_data = (
                s3_client.get_object(Bucket=SEIL_S3_BUCKET, Key=csv_key)["Body"]
                .read()
                .decode("utf-8")
            )

            # Parse CSV data
            csv_rows = list(csv.reader(StringIO(csv_data)))

            # Construct SOAP request
            soap_request = construct_soap_request(csv_rows)

            # Send SOAP request to TechOne (replace this with your actual logic)
            # techone_response = send_to_techone(soap_request)

            # Move the file to the 'delivered' folder
            move_file(csv_key, "delivered")

        except Exception as e:
            print(f"Error processing message {msg['messageId']}: {str(e)}")
            result["batchItemFailures"].append({"itemIdentifier": msg["messageId"]})

            # Move the file to the 'error' folder
            move_file(csv_key, "error")

    return result

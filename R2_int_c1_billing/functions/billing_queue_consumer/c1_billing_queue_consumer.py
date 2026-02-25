import logging
import json
import os
import boto3
import csv
import io
from io import StringIO
import datetime
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET

logger = logging.getLogger("Billing Queue Consumer")
logger.setLevel(logging.INFO)

DEFAULT_REGION = "ap-southeast-2"  # Sydney


def listToString(s):

    # initialize an empty string
    str1 = ""

    # traverse in the string
    for ele in s:
        str1 += ele + ","

    # return string
    return str1


def get_techone_soap_secret(secret_name):
    """
    Function to fetch secret value for file transfer
    """
    logger.info("Entering get_techone_soap_secret()")
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])
    secret_dict = {
        "user_id": secret["UserId"],
        "password": secret["Password"],
        "wsdl": secret["WSDL"],
        "config": secret["Config"],
    }
    logger.info("Existing get_techone_soap_secret()")
    return secret_dict


def replace_all(text, dic):
    """
    Utility function to relace string based on dictionary
    """
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


def change_date_format(row_billing):
    """
    Function to change date to dd/mm/yyyy format
    """
    try:
        source_format = "%d%m%Y"
        target_format = "%d/%m/%Y"
        # row_billing_str_split = row_billing_str.split(",")
        i_start_date = row_billing["Start Date"]
        i_start_date_len = len(row_billing["Start Date"])
        # logger.info(f"i_start_date_len :{i_start_date_len}")
        if i_start_date_len == 7:
            i_start_date = "0" + str(i_start_date)
        i_end_date = row_billing["End Date"]
        i_end_date_len = len(row_billing["End Date"])

        # logger.info(f"i_end_date_len :{i_end_date_len}")
        if i_end_date_len == 7:
            i_end_date = "0" + str(i_end_date)
        i_campaign_start_date = row_billing["Campaign Billing Start Date"]
        i_campaign_start_date_len = len(i_campaign_start_date)
        # logger.info(f"i_campaign_start_date_len: {i_campaign_start_date_len}")
        i_campaign_end_date = row_billing["Campaign Billing End Date"]
        i_campaign_end_date_len = len(i_campaign_end_date)
        # logger.info(f"i_campaign_end_date_len: {i_campaign_end_date_len}")
        if i_campaign_start_date_len == 7:
            i_campaign_start_date = "0" + str(i_campaign_start_date)
        if i_campaign_end_date_len == 7:
            i_campaign_end_date = "0" + str(i_campaign_end_date)
        start_date = datetime.datetime.strptime(i_start_date, source_format).strftime(
            target_format
        )
        end_date = datetime.datetime.strptime(i_end_date, source_format).strftime(
            target_format
        )
        campaign_start_date = datetime.datetime.strptime(
            i_campaign_start_date, source_format
        ).strftime(target_format)
        campaign_end_date = datetime.datetime.strptime(
            i_campaign_end_date, source_format
        ).strftime(target_format)
        # replace_dic = {
        #    i_start_date: start_date,
        #    i_end_date: end_date,
        #    i_campaign_start_date: campaign_start_date,
        #    i_campaign_end_date: campaign_end_date,
        # }
        # row_billing_str = replace_all(row_billing_str, replace_dic)
        row_billing["Start Date"] = start_date
        row_billing["End Date"] = end_date
        row_billing["Campaign Billing Start Date"] = campaign_start_date
        row_billing["Campaign Billing End Date"] = campaign_end_date

        return row_billing
    except Exception as e:
        print(f"Error converting in row: {row_billing}. Error: {e}")


def str_field_handling(row_billing):
    """
    Function to add double quotes to the string fields.
    Numeric and Date fields will be ignored
    """
    # row_billing_str_split = row_billing_str.split(",")
    # Double quote will be added to belowString field
    business_area_code = row_billing["Business Area"]
    sales_rep = row_billing["Sales Rep"]
    sales_group = row_billing["Sales Group"]
    sales_office = row_billing["Sales Office"]
    transaction_type = row_billing["Transaction Type"]
    goods_received = row_billing["Goods Received"]
    campaign_type = row_billing["Campaign Type"]
    crmid = row_billing["CRMID"]
    billing_account_name = row_billing["Billing Account Name"]
    primary_advertiser = row_billing["Primary Advertiser"]
    campaign_name = row_billing["Campaign Name"]
    revenue_type = row_billing["Revenue Type"]
    invoice_currency = row_billing["Invoice Currency"]
    external_po_number = row_billing["PO Number"]
    subtotal_salesarea_code = row_billing["Subtotal \x96 Sales Area Code"]
    subtotal_salesarea = row_billing["Subtotal \x96 Sales Area"]
    tax = row_billing["Tax"]

    # Double quotes should not be added to Numeric and Date fields.
    # Numeric fields
    general_ledger_code = row_billing["General Ledger Code"]
    campaign_reference = row_billing["Campaign Reference"]
    invoice_number = row_billing["Invoice Number"]
    line_number = row_billing["Line Number"]
    subtotal_line = row_billing["Subtotal Line"]
    agency_commision = row_billing["Agency Commission"]

    # Date Column
    i_start_date = row_billing["Start Date"]
    i_end_date = row_billing["End Date"]
    i_campaign_start_date = row_billing["Campaign Billing Start Date"]
    i_campaign_end_date = row_billing["Campaign Billing End Date"]

    journal_comments = row_billing["Journal Comments"]
    journal_type = row_billing["Journal Type"]
    product_code = row_billing["Product Code"]
    product_name = row_billing["Product Name"]

    field_list = [
        general_ledger_code,
        business_area_code,
        sales_rep,
        sales_group,
        sales_office,
        transaction_type,
        goods_received,
        campaign_type,
        crmid,
        billing_account_name,
        primary_advertiser,
        i_start_date,
        i_end_date,
        campaign_reference,
        campaign_name,
        revenue_type,
        invoice_currency,
        invoice_number,
        line_number,
        external_po_number,
        subtotal_salesarea_code,
        subtotal_salesarea,
        tax,
        subtotal_line,
        agency_commision,
        i_campaign_start_date,
        i_campaign_end_date,
        journal_comments,
        journal_type,
        product_code,
        product_name,
    ]
    modified_row_billing_str = ""

    for index, field in enumerate(field_list):
        # Double quotes will not be added for below fields
        if index in [0, 11, 12, 13, 17, 18, 23, 24, 25, 26, 28, 29]:
            modified_row_billing_str += field + ","
        else:
            field_quote = f'"{field}"'
            modified_row_billing_str += field_quote + ","

    modified_row_billing_str = modified_row_billing_str[:-1]
    return modified_row_billing_str


def construct_soap_request(user_id, password, config, csv_reader_list, first_file_flag):
    """
    Function to construct Techone SOAP Request.
    General Ledger Code  -->  GENERALLEDGERCODE
    Revenue Type  -->  REVENUETYPE
    Goods Received	->  GOODSRECEIVED
    Sales Rep	--> SALESREP
    Campaign Type	 --> CAMPAIGNTYPE
    Activity Code	 --> ACTIVITYCODE
    Campaign Name	-->  CAMPAIGNNAME
    Billing Account Name	-->  BILLINGACCOUNTNAME
    Primary Advertiser	-->  PRIMARYADVERTISER
         Invoice Currency	-->  INVOICECURRENCY
    Campaign Reference	-->  CAMPAIGNREFERENCE
    External PO Number	-->  EXTERNALPONUMBER
         CRMID	-->  CRMID
    Tax  -->  Tax
    Subtotal Line	-->  SUBTOTALLINE
    Invoice Number	-->  INVOICENUMBER
    Campaign Billing Start Date	-->  CAMPAIGNBILLINGSTARTDATE
    Campaign Billing End Date	-->  CAMPAIGNBILLINGENDDATE
    Transaction Type	--> TRANSACTIONTYPE
    Sales Group	-->  SALESGROUP
    Sales Office	-->  SALESOFFICE
    """

    soap_request = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        + "<soapenv:Envelope"
        + ' xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        + ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        + ' xmlns:ns1="http://TechnologyOneCorp.com/Public/Services"> '
        + "   <soapenv:Header/> "
        + "   <soapenv:Body> "
        + "     <ns1:Warehouse_DoImport> "
        + '         <ns1:request WarehouseName="SALESFORCE" WarehouseTableName="BILLINGDATA" ImportMode="MixedMode"> '
        + '            <ns1:Auth UserId="'
        + user_id
        + '" Password="'
        + password
        + '" Config="'
        + config
        + '" FunctionName="$E1.BI.WHT.DOIMP.WS"/> '
        + "            <ns1:Columns> "
        + '               <ns1:ColumnInfo Name="GENERALLEDGERCODE"/>'
        + '               <ns1:ColumnInfo Name="BUSINESSAREACODE"/>'
        + '               <ns1:ColumnInfo Name="SALESREP"/>'
        + '               <ns1:ColumnInfo Name="SALESGROUP"/> '
        + '               <ns1:ColumnInfo Name="SALESOFFICE"/> '
        + '               <ns1:ColumnInfo Name="TRANSACTIONTYPE"/> '
        + '               <ns1:ColumnInfo Name="GOODSRECEIVED"/>'
        + '               <ns1:ColumnInfo Name="CAMPAIGNTYPE"/>'
        + '               <ns1:ColumnInfo Name="CRMID"/>'
        + '               <ns1:ColumnInfo Name="BILLINGACCOUNTNAME"/>'
        + '               <ns1:ColumnInfo Name="PRIMARYADVERTISER"/>'
        + '               <ns1:ColumnInfo Name="STARTDATE"/> '
        + '               <ns1:ColumnInfo Name="ENDDATE"/> '
        + '               <ns1:ColumnInfo Name="CAMPAIGNREFERENCE"/>'
        + '               <ns1:ColumnInfo Name="CAMPAIGNNAME"/>'
        + '               <ns1:ColumnInfo Name="REVENUETYPE"/>'
        + '               <ns1:ColumnInfo Name="INVOICECURRENCY"/>'
        + '               <ns1:ColumnInfo Name="INVOICENUMBER"/>'
        + '               <ns1:ColumnInfo Name="LINENUMBER"/>'
        + '               <ns1:ColumnInfo Name="EXTERNALPONUMBER"/>'
        + '               <ns1:ColumnInfo Name="SUBTOTALSALESAREACODE"/>'
        + '               <ns1:ColumnInfo Name="SUBTOTALSALESAREA"/>'
        + '               <ns1:ColumnInfo Name="TAX"/>'
        + '               <ns1:ColumnInfo Name="SUBTOTALLINE"/>'
        + '               <ns1:ColumnInfo Name="AGENCYCOMMISSION"/>'
        + '               <ns1:ColumnInfo Name="CAMPAIGNBILLINGSTARTDATE"/> '
        + '               <ns1:ColumnInfo Name="CAMPAIGNBILLINGENDDATE"/> '
        + '               <ns1:ColumnInfo Name="JOURNALCOMMENTS"/> '
        + '               <ns1:ColumnInfo Name="JOURNALTYPE"/> '
        + '               <ns1:ColumnInfo Name="PRODUCTCODE"/> '
        + '               <ns1:ColumnInfo Name="PRODUCTNAME"/> '
        + "            </ns1:Columns> "
        + "            <ns1:Rows> "
        + "            </ns1:Rows> "
        + "         </ns1:request> "
        + "      </ns1:Warehouse_DoImport> "
        + "   </soapenv:Body> "
        + "</soapenv:Envelope> "
    )

    file_object = io.StringIO(soap_request)
    tree = ET.parse(file_object)

    root = tree.getroot()
    print(root)
    # ET.indent(root)
    print(ET.tostring(root))
    # logger.info

    body = tree.find("ns0:Body")
    print(body)

    body = root[1]
    warehouse = body[0]
    request = warehouse[0]
    rows = request[2]
    record_count = 0

    for row_billing in csv_reader_list:
        # csv_header_key is the header keys which you have defined in your csv header
        # row_billing_str = listToString(row_billing)
        # row_billing_str = change_date_format(row_billing_str)
        # row_billing_str = str_field_handling(row_billing_str)

        record_crmid = row_billing["CRMID"]
        record_crmid = record_crmid.strip()
        if record_crmid is not None and record_crmid != "":
            row_billing = change_date_format(row_billing)
            row_billing_str = str_field_handling(row_billing)

            row = ET.Element("ns1:Row")
            row.text = row_billing_str
            rows.append(row)
            record_count = record_count + 1
        else:
            logger.info(f"Ignore - CRM ID is Null:{row_billing}")
    logger.info("XML to TechOne")
    logger.info(f"Record Count in Construct stage: {record_count}")
    logger.info(ET.tostring(root))
    return root


def read_data_from_s3(file_path):
    """
    Function to read data from S3 bucket
    """
    billing_bucket = os.environ["BILLING_BUCKET"]
    s3_client = boto3.client("s3", region_name=DEFAULT_REGION)
    logger.info("Getting Object from S3")
    data = s3_client.get_object(Bucket=billing_bucket, Key=file_path)
    contents = data["Body"].read().decode("utf-8").splitlines()
    # contents = data["Body"].read().decode("windows-1252").splitlines()
    csv_reader = csv.DictReader(
        contents, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
    )
    parsed_data = list(csv_reader)

    # Check for double quotes and log
    cleaned_data = []
    for row in parsed_data:
        cleaned_row = {}
        for key, value in row.items():
            if isinstance(value, str):
                if '"' in value:  # Check if double quotes exist
                    logger.info(
                        f'Double quotes found in key "{key}", value: "{value}", row: "{row}"'
                    )
                    cleaned_row[key] = value.replace('"', "")  # remove the quotes.
                else:
                    cleaned_row[key] = value
            else:
                cleaned_row[key] = value
        cleaned_data.append(cleaned_row)

    return cleaned_data


def lambda_handler(event, context):
    logger.info("Billing Queue Consumer")
    logger.info(event)

    techone_soap_secret_name = os.environ["TECHONE_SOAP_SECRET_NAME"]

    techone_soap_dic = get_techone_soap_secret(techone_soap_secret_name)
    user_id = techone_soap_dic["user_id"]
    password = techone_soap_dic["password"]
    wsdl = techone_soap_dic["wsdl"]
    config = techone_soap_dic["config"]

    records = event["Records"][0]
    logger.info(records)
    file_path = event["Records"][0]["body"]
    logger.info(file_path)
    if file_path.endswith("_1.csv"):
        first_file_flag = True
    else:
        first_file_flag = False
    csv_reader_list = read_data_from_s3(file_path)
    logger.info("Before Content")
    logger.info(csv_reader_list)
    root_xml = construct_soap_request(
        user_id, password, config, csv_reader_list, first_file_flag
    )
    logger.info(type(root_xml))
    logger.info(root_xml)
    root_xml_str = ET.tostring(root_xml)
    # logger.info(root_xml_str)
    # print(ET.tostring(root_xml, encoding='utf8').decode('utf8'))

    lambda_client = boto3.client("lambda")
    techone_request = {
        "content": {"soap_url": wsdl, "content": root_xml_str.decode()},
    }

    techone_response = lambda_client.invoke(
        FunctionName=os.environ["TECHONE_ADAPTOR_FUNCTION"],
        InvocationType="RequestResponse",
        Payload=json.dumps(techone_request),
    )

    logger.info(techone_response)
    logger.info(techone_response["ResponseMetadata"]["HTTPStatusCode"])
    logger.info(techone_response["Payload"])

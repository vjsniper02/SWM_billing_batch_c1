from unittest import mock

import json
import io
import os
import sys

sys.path.append(
    os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
)  # project root folder

from tests.mock_boto import mock_client_generator


MOCK_ENV = {
    "TECHONE_SOAP_SECRET_NAME": "techone-soap-secret-name",
    "BILLING_BUCKET": "billing-bucket",
    "TECHONE_ADAPTOR_FUNCTION": "techone-adaptor-function",
}


class atdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


FILE_NAME = "FINANCE_20240510113633.csv"


@mock.patch.dict("os.environ", MOCK_ENV, clear=True)
class TestBillingQueueConsumer:

    def test_lambda_handler(self):

        def mock_boto3_session():
            client = mock.MagicMock()
            client.get_secret_value.return_value = {
                "SecretString": json.dumps(
                    {
                        "UserId": "user-id",
                        "Password": "password",
                        "WSDL": "wsdl",
                        "Config": "config",
                    }
                )
            }

            result = mock.MagicMock()
            result.client.return_value = client
            return result

        class MockS3Client:
            def __init__(mock_self, region_name) -> None:
                pass

            def get_object(mock_self, Bucket, Key):
                f = open(os.path.dirname(__file__) + "/" + FILE_NAME, "r")
                return {"Body": io.BytesIO(f.read().encode("utf-8"))}

        class MockLambdaClient:
            def __init__(mock_self, region_name="") -> None:
                pass

            def invoke(mock_self, FunctionName, InvocationType, Payload):
                return {"ResponseMetadata": {"HTTPStatusCode": 200}, "Payload": ""}

        with mock.patch("boto3.session.Session", mock_boto3_session), mock.patch(
            "boto3.client",
            mock_client_generator({"s3": MockS3Client, "lambda": MockLambdaClient}),
        ):
            GOOD_EVENT = {"Records": [{"body": ""}]}

            from functions.billing_queue_consumer.c1_billing_queue_consumer import (
                lambda_handler,
            )

            lambda_handler(GOOD_EVENT, {})

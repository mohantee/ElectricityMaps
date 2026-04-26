import os

import boto3
import pytest
from moto import mock_aws

from electricity_maps.config import Settings


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="electricity-maps")
        yield conn

@pytest.fixture
def test_settings(tmp_path):
    """Provides settings for tests using a local temporary directory for data.

    Using local paths avoids the 'Generic S3 error' when deltalake-rs tries
    to access S3 in a mocked environment without a full moto server.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    return Settings(
        api_key="test_key",
        api_base_url="https://api.test.com/v4",
        emaps_data_dir=str(data_dir),
        zone="FR",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_region="us-east-1"
    )

@pytest.fixture
def sample_mix_response():
    return {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-04-23T09:00:00.000Z",
                "updatedAt": "2026-04-24T10:26:32.718Z",
                "isEstimated": True,
                "estimationMethod": "SANDBOX_MODE_DATA",
                "mix": {
                    "nuclear": 27069.648,
                    "biomass": 925.166,
                    "coal": 0,
                    "wind": 2686.155,
                    "solar": 9533.531,
                    "hydro": 3440.41,
                    "gas": 328.065,
                    "oil": 40.678,
                    "hydro storage": {"charge": 1239.355, "discharge": None},
                    "battery storage": {"charge": 24.72, "discharge": None},
                    "flows": {"exports": 8573, "imports": 348}
                }
            }
        ]
    }

@pytest.fixture
def sample_flows_response():
    return {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-04-23T09:00:00.000Z",
                "updatedAt": "2026-04-24T10:26:32.718Z",
                "import": {"BE": 561, "CH": 197},
                "export": {"DE": 335, "GB": 3016}
            }
        ]
    }

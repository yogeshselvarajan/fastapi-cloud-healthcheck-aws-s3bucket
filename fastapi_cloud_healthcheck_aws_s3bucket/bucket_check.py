import boto3
from botocore.exceptions import ClientError
from fastapi_cloud_healthcheck import HealthCheckBase, HealthCheckStatusEnum


class HealthCheckS3Bucket(HealthCheckBase):
    def __init__(self, bucket_name: str, region: str) -> None:
        super().__init__()
        self._identifier = bucket_name
        self._bucket_name = bucket_name
        self._metadata = {
            "provider": "aws",
            "region": region,
            "category": "storage",
            "serviceName": "S3",
            "accountId": boto3.client('sts').get_caller_identity()['Account']
        }
        self._statusMessages = {}

    def __checkHealth__(self) -> HealthCheckStatusEnum:
        """Check the health of the S3 bucket."""

        # Create the S3 client
        try:
            s3_client = boto3.client('s3')
            self._statusMessages['clientCreation'] = "S3 client created successfully."
        except Exception as e:
            self._statusMessages['clientCreation'] = f"Failed to initialize S3 client: {str(e)}"
            return HealthCheckStatusEnum.UNHEALTHY

        # Check if the bucket exists and is accessible
        try:
            s3_client.head_bucket(Bucket=self._bucket_name)
            self._statusMessages['bucketCheck'] = "Bucket exists and is accessible."
        except ClientError as e:
            self._statusMessages['bucketCheck'] = f"Bucket not found or inaccessible: {e.response['Error']['Message']}"
            return HealthCheckStatusEnum.UNHEALTHY

        # Object operations (upload, read, delete)
        test_key = "health_check_test_object"
        test_content = b"health check test content"

        try:
            # Upload the test object
            s3_client.put_object(Bucket=self._bucket_name, Key=test_key, Body=test_content)
            self._statusMessages['objectUpload'] = "Test object uploaded successfully."

            # Read the test object
            response = s3_client.get_object(Bucket=self._bucket_name, Key=test_key)
            response_body = response['Body'].read()

            if response_body != test_content:
                raise ValueError("Test object content mismatch.")
            self._statusMessages['objectRead'] = "Test object content matches."

            # Cleanup the test object
            s3_client.delete_object(Bucket=self._bucket_name, Key=test_key)
            self._statusMessages['objectCleanup'] = "Test object cleaned up successfully."

        except ClientError as e:
            self._statusMessages['objectUpload'] = f"Error during object operation: {e.response['Error']['Message']}"
            return HealthCheckStatusEnum.UNHEALTHY
        except ValueError as ve:
            self._statusMessages['objectRead'] = str(ve)
            return HealthCheckStatusEnum.UNHEALTHY

        # Check multipart upload functionality
        try:
            response = s3_client.create_multipart_upload(Bucket=self._bucket_name, Key='multipart_test_object')
            upload_id = response['UploadId']
            s3_client.abort_multipart_upload(Bucket=self._bucket_name, Key='multipart_test_object', UploadId=upload_id)
            self._statusMessages['multipartUpload'] = "Multipart upload is functional."
        except ClientError as e:
            self._statusMessages['multipartUpload'] = f"Multipart upload failed: {e}"
            return HealthCheckStatusEnum.UNHEALTHY

        # Check bucket policy
        try:
            s3_client.get_bucket_policy(Bucket=self._bucket_name)
            self._statusMessages['bucketPolicy'] = "Bucket policy retrieved successfully."
        except ClientError as e:
            self._statusMessages['bucketPolicy'] = f"Bucket policy not found: {e.response['Error']['Message']}"

        return HealthCheckStatusEnum.HEALTHY

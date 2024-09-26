# fastapi_cloud_healthcheck_aws_s3bucket

A FastAPI-based health check module for monitoring the health of AWS S3 buckets using the `fastapi_cloud_healthcheck` package.

## Features

* Bucket Accessibility: Verifies if the S3 bucket exists and is accessible within the given AWS region.
* Object Operations: Performs test operations such as uploading, reading, and deleting objects within the bucket to ensure correct permissions.
* Bucket Policy Validation: Checks for the presence of an S3 bucket policy and its retrieval status.
* Automated Account Detection: Automatically retrieves the AWS Account ID for the health check metadata.
* Timezone Support: Logs the last checked timestamp in Indian Standard Time (IST). 

## Adding Health Checks

Here is a sample FastAPI application that integrates the S3 bucket health check:

```python
from fastapi import FastAPI
from fastapi_cloud_healthcheck import HealthCheckFactory, healthCheckRoute
from fastapi_cloud_healthcheck_aws_s3bucket import HealthCheckS3Bucket

app = FastAPI()

# Create Health Check Factory
health_check_factory = HealthCheckFactory()

# Add the S3 Bucket Health Check
health_check_factory.add(
    HealthCheckS3Bucket(
        bucket_name="my-sample-bucket",
        region="us-west-1"
    )
)

# Add the health check route to FastAPI
app.add_api_route('/health', endpoint=healthCheckRoute(factory=health_check_factory))

# Start the FastAPI server using Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=5000)
```

In the above example:
- We create a health check for the S3 bucket named `my-sample-bucket` in the region `us-west-1`. 
- The health check endpoint `/health` will expose detailed health information about the S3 bucket.

## Health Check Process
The **S3 Bucket Health Check** performs the following operations:

1. **Bucket Accessibility**: Uses the `head_bucket` boto3 API to check if the bucket exists and is accessible.
  
2. **Test Object Operations**:
    - **Upload**: Uploads a temporary object to the bucket.
    - **Read**: Retrieves and validates the content of the test object.
    - **Cleanup**: Deletes the test object after validation.
  
3. **Bucket Policy Check**: Ensures the bucket policy is accessible by attempting to retrieve it.

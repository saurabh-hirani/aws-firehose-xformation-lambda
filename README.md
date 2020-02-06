# aws-firehose-xformation-lambda

AWS Kinesis Firehose transformation Lambda required for https://github.com/saurabh-hirani/terraform-aws-firehose-elasticsearch

Table of contents:

* [Pre-requisites](#pre-requisites)
* [Local setup](#local-setup)
* [Sample stats output](#sample-stats-output)
* [Package for uploading to AWS](#package-for-uploading-to-aws)

## Pre-requisites

Understand the need for having an [AWS Kinesis transformation Lambda](https://aws.amazon.com/blogs/compute/amazon-kinesis-firehose-data-transformation-with-aws-lambda/)

## Local setup

1. Setup and activate virtualenv as per instructions provided [here](https://gist.github.com/saurabh-hirani/3a2d582d944a792d0e896892e0ee0dea)

2. Test with localhost payloads

    ```sh
    python3 xformation_lambda.py localhost_payload_1.json 2>&1  | jq
    python3 xformation_lambda.py localhost_payload_2.json 2>&1  | jq
    ```

## Sample stats output

Running above sample payloads or actual xformation payloads with AWS Firehose will log sample stats like the following which
can be analyzed through AWS Cloudwatch insights:

```sh
{
  "level": "INFO",
  "funcName": "lambda_handler",
  "lineno": 158,
  "message": "xformation_stats",
  "firehose_name": "arn:aws:firehose:us-east-1:123456678:deliverystream/test-firehose-delivery-stream",
  "total_records": 2,
  "total_processed": 2,
  "total_failed": 0,
  "total_failed_max_size_exceeded": 0,
  "total_failed_b64_decode": 0,
  "total_failed_json_load": 0,
  "total_failed_xformation": 0,
  "total_event_record_size_bytes": 184,
  "max_event_record_size_bytes": 92,
  "min_event_record_size_bytes": 92,
  "all_records_processed": true,
  "index_dates": "2020-02-01",
  "timestamp": "2020-02-02T00:00:01.722613Z"
}
```

The field ```index_dates``` parses the record timestamp field (as specified in the ```TIMESTAMP_KEY``` input)
and gets the actual date for which the record is destined. This approach selects the right rolling index because
going with the timestamp at which Firehose gets the record is incorrect because the Firehose may buffer, retry and process a
record on ```2020-01-02 00:00:01``` with actual log timestamp ```2020-01-01 23:59:59```.

The reason the field name is plural - ```index_dates``` as opposed to ```index_date``` is because the Firehose payload batch (AWS Kinesis
firehose buffers data and processes in batch) may contain records for both current and next at the rolling date point i.e. one record may
have log timestamp ```2020-01-01 23:59:59``` and another might have ```2020-01-02 00:00:00``` in which case the stats logging output will
look like the following:

```sh
{
  "level": "INFO",
  "funcName": "lambda_handler",
  "lineno": 158,
  "message": "xformation_stats",
  "firehose_name": "arn:aws:firehose:us-east-1:123456678:deliverystream/test-firehose-delivery-stream",
  "total_records": 2,
  "total_processed": 2,
  "total_failed": 0,
  "total_failed_max_size_exceeded": 0,
  "total_failed_b64_decode": 0,
  "total_failed_json_load": 0,
  "total_failed_xformation": 0,
  "total_event_record_size_bytes": 184,
  "max_event_record_size_bytes": 92,
  "min_event_record_size_bytes": 92,
  "all_records_processed": true,
  "index_dates": "2020-02-01,2020-01-02",
  "timestamp": "2020-02-02T00:00:00.722613Z"
}
```

## Package for uploading to AWS

- Run the following command

  ```sh
  # ./package.sh xformation_lambda.py
  ```

  This will create a file ```xformation_lambda.zip``` which can be used to upload to AWS Lambda

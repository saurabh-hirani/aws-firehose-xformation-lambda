# aws-firehose-xformation-lambda

AWS Kinesis Firehose transformation Lambda

## Why?

Pending

## Local setup

1. Setup and activate virtualenv as per instructions provided [here](https://gist.github.com/saurabh-hirani/3a2d582d944a792d0e896892e0ee0dea)

2. Test with localhost payloads

    ```sh
    python3 xformation_lambda.py localhost_payload_1.json 2>&1  | jq
    python3 xformation_lambda.py localhost_payload_2.json 2>&1  | jq
    ```

## Package for uploading to AWS

```sh
# ./package.sh xformation_lambda.py
```

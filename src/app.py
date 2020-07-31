import json
import email
import base64
import boto3
import pandas as pd
import os


tmp_dir = "/tmp/"
out_dir = "output/"
bucket_name = "nyk-email-box"

# Reading attachment from email
def read_attachment(email_list):
    msg_payload_list = []
    for email_info in email_list:
        msg = email.message_from_file(open(tmp_dir + email_info))
        msg_payload = msg.get_payload()[1]
        msg_payload_list.append(msg_payload)
        attachment = msg_payload._payload
        decoded_data = base64.b64decode(attachment)
        output_file = open(tmp_dir + msg_payload.get_filename(), 'wb')
        output_file.write(decoded_data)
        output_file.close()
    return msg_payload_list

# Reading excel file and extract
def read_excel(msg_payload_list):
    for msg_payload in msg_payload_list:
        df = pd.read_excel(tmp_dir + msg_payload.get_filename())
        for row in df.values:
            print(row)

def main():
    s3_client = boto3.client("s3")
    list_objects = s3_client.list_objects(Bucket=bucket_name)
    contents = list_objects["Contents"]
    email_list = []

    # Getting file from S3
    for content in contents:
        key_id = content["Key"]
        email_list.append(key_id)
        s3_client.download_file(Bucket=bucket_name, Key=key_id, Filename=tmp_dir + key_id)

    # Output attachment to tmp
    msg_payload_list = read_attachment(email_list)

    # Reading excel file
    read_excel(msg_payload_list)


def lambda_handler(event, context):
    main()


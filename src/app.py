import json
import email
import base64
import boto3
import pandas as pd
import os
import psycopg2
from data_base import Database

tmp_dir = "/tmp/"
bucket_name = "email-box"
dbname='xxxx'
user='xxxx'
password='password'
host='xxxx.xxxx.ap-northeast-1.rds.amazonaws.com'
port='5432'



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
    db = Database(dbname, user, password, host, port)
    for msg_payload in msg_payload_list:
        df = pd.read_excel(tmp_dir + msg_payload.get_filename())
        for row in df.values:
            update_excel_to_db(row,db)
    # Close the connection
    db.close()

# Update data to Postgres
def update_excel_to_db(data,db):
    data_postgres = tuple(data)
    db.insert('INSERT INTO contenttable (name, value1, value2, value3) VALUES (%s, %s, %s, %s)', data_postgres)

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


import json
import email
import base64
import boto3
import pandas as pd
import os


tmp_dir = "/tmp/"

def lambda_handler(event, context):

    # Getting file from S3 Email box
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("nyk-email-box")
    bucket.download_file('kl0orimj1t164dj0v0p212kep8lm92bhclects01', tmp_dir + 'kl0orimj1t164dj0v0p212kep8lm92bhclects01')
    path = os.path.abspath(tmp_dir + 'kl0orimj1t164dj0v0p212kep8lm92bhclects01')

    # Open email file and get excel attachment
    msg = email.message_from_file(open(tmp_dir + 'kl0orimj1t164dj0v0p212kep8lm92bhclects01'))
    payload = msg.get_payload()[1]
    attachment = payload._payload

    # decoding attachment file and writing to binary file
    decoded_data = base64.b64decode(attachment)
    output_file = open(tmp_dir + payload.get_filename(), 'wb')
    output_file.write(decoded_data)
    output_file.close()

    df = pd.read_excel(tmp_dir + payload.get_filename())
    for row in df.values:
        print(row)


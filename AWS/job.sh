#!/bin/sh
rm ~/.aws/credentials
python3 credentials.py
python3 AWS_Master.py

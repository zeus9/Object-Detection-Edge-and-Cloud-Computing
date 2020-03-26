import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')

file_name = '/home/ubuntu/.aws/credentials'
object_name = 'credentials'
bucket = 'raspiicredentials'
try:
     response = s3_client.download_file(bucket, object_name, file_name)
     print(response)
except ClientError as e:
    print(e)
print('Saved {object} from {bucket} to {file}'.format(object=object_name, bucket=bucket, file=file_name))


#client = boto3.client('sqs')

#response = client.get_queue_attributes(

#            QueueUrl="https://sqs.us-east-1.amazonaws.com/029834165530/video_queue",

#            AttributeNames=[

               # 'All'

            #]

        #)
#print(response)

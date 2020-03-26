import boto3
from botocore.exceptions import ClientError
import subprocess
import time


class AWS:
    def __init__(self, queue='https://sqs.us-east-1.amazonaws.com/029834165530/image_queue', bucket_im='raspiimages',
                 bucket_res='raspiresults', home = '/home/ubuntu/' ,master = 'i-0fc3b6a0ade7dedcb'):
        self.queue = queue
        self.bucket_im = bucket_im
        self.bucket_res = bucket_res
        self.home = home
        self.master = master
        self.stop = 0

    # Method to receive SQS message
    def receiveMessage(self, QueueUrl):
        sqs = boto3.client('sqs')
        # receive one message
        response = sqs.receive_message(
            QueueUrl=QueueUrl,
            AttributeNames=[
                'All'
            ],
            MaxNumberOfMessages=1,
            VisibilityTimeout=30,
            WaitTimeSeconds=5,
        )
        print(response)
        if 'Messages' in response:
            message = response.get('Messages')[0]
            return message.get('Body'), message.get('ReceiptHandle')
        else:
            print('no messages')

    
    def queueNumberOfMessages(self):
        client = boto3.client('sqs')
        response = client.get_queue_attributes(
            QueueUrl=self.queue,
            AttributeNames=[
                'All'
            ]
        )
        # print(response.get('Attributes').get('ApproximateNumberOfMessages'))

        return response.get('Attributes')
    
    def deleteMessage(self, QueueUrl, ReceiptHandle):
        sqs = boto3.client('sqs')

        try:
            response = sqs.delete_message(
                QueueUrl=QueueUrl,
                ReceiptHandle=ReceiptHandle
            )
            print(response)
            print('Delete message successfully')
        except Exception as e:
            print(e)
            print('Can not delete message')

    # download video from S3
    def downloadS3(self, s3_client, bucket, object_name, file_name=None):
        try:
            _ = s3_client.download_file(bucket, object_name, file_name)
        except ClientError as e:
            print(e)
        print('Saved {object} from {bucket} to {file}'.format(object=object_name, bucket=bucket, file=file_name))

    # Method to upload to S3
    def uploadS3(self, s3_client, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name
        try:
            _ = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
        print('Saved {file} to {bucket}'.format(file=file_name, bucket=bucket))



    # Get all stopped instances
    def instanceState(self, instance_name, state):
        ec2 = boto3.resource('ec2')

        instances = ec2.instances.filter(
            # instance-id
            Filters=[{'Name': instance_name, 'Values': state}])
        return instances

    # Start instances
    def lambda_handler_start(self, event, InstanceIds):
        ec2 = boto3.client('ec2')

        print('InstanceIds', InstanceIds)
        if event == 'start':
            ec2.start_instances(InstanceIds=InstanceIds)
            # ec2.instances.filter(InstanceIds=instances).start()
        else:
            ec2.stop_instances(InstanceIds=InstanceIds)
            # ec2.instances.filter(InstanceIds=instances).stop()

        print(event + ' your instances: ' + str(InstanceIds))
        return InstanceIds


        # Instance Manager: To check if a new instance must be spawned based on SQS queue
    def instanceManager(self):
        # Check how many instaces are in start and stop state
        #download key
        #self.downloadS3(self.key, 'raspiicredentials', self.home + '.aws/' + self.key)

        instances = self.instanceState('instance-state-name', ['pending','running', 'stopped'])
        # Count the number of running instances
        instance_cnt = -1 ## due to master node
        # instance_id = []
        stopped_instances = []
        for instance in instances:
            if instance.state['Name'] in {'pending','running'} :
                instance_cnt = instance_cnt + 1
            else:
                stopped_instances.append(instance.id)
        
        # Now check how many pending SQS messages have yet to be addressed
        response = self.queueNumberOfMessages()
        # Check if the number of unread messages is greater than running instances
        print(response['ApproximateNumberOfMessages'], response['ApproximateNumberOfMessagesNotVisible'], self.stop, instance_cnt)
        if int(response['ApproximateNumberOfMessages']) > instance_cnt:
            self.stop = 0
            # Start instances based on difference of messages and instance count
            instance_diff = int(response['ApproximateNumberOfMessages']) - instance_cnt
            if len(stopped_instances[:instance_diff]) > 0:
                self.lambda_handler_start('start', stopped_instances[:instance_diff])
                # stopped_instances = stopped_instances[instance_diff:]
        elif int(response['ApproximateNumberOfMessages']) == 0 and int(response['ApproximateNumberOfMessagesNotVisible']) == 0:
            self.stop += 1

            instances = self.instanceState('instance-state-name', ['running'])
    
            runnings_instances = []
            for instance in instances:
                if instance.id != self.master:
                    runnings_instances.append(instance.id)
            if len(runnings_instances) > 0 and self.stop > 20:
                self.lambda_handler_start('stop', runnings_instances)
        else:
            self.stop = 0


    def credentials(self):
        client = boto3.client('sts')

        response = client.assume_role(
            RoleArn='arn:aws:iam::029834165530:role/kpyla',
            RoleSessionName='kpyla',
        )
        print(response)

#         iam = boto3.client('iam')

#         paginator = iam.get_paginator('list_users')
#         for page in paginator.paginate():
#             for user in page['Users']:
#                 print("User: {0}\nUserID: {1}\nARN: {2}\nCreatedOn: {3}\n".format(
#                 user['UserName'],
#                 user['UserId'],
#                 user['Arn'],
#                 user['CreateDate']
#             )
#  )


aws = AWS()
# aws.AwsDetection()
# print(aws.queueNumberOfMessages())
while True:
    aws.instanceManager()
    time.sleep(1)


# tryPaginate()

# print(aws.credentials())





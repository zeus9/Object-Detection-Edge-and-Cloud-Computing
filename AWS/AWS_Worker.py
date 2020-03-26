import boto3
from botocore.exceptions import ClientError
import subprocess
import time
from boto3 import Session

class AWS:
    def __init__(self, queue='https://sqs.us-east-1.amazonaws.com/029834165530/image_queue', bucket_im='raspiimages',
                 bucket_res='raspiresults', home = '/home/ubuntu/'):
        self.master = True
        self.queue = queue
        self.bucket_im = bucket_im
        self.bucket_res = bucket_res
        self.home = home
        self.key = 'credentials'

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
            return None, None

    
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

    # Run Darknet Object detection
    def objDet(self, videopath):
        cwd = self.home + "darknet"
        # home = "/home/ubuntu/"
        videopath = self.home + videopath
        command = "./darknet detector test cfg/coco.data " \
                  "cfg/yolov3-tiny.cfg yolov3-tiny.weights {v}".format(v=videopath, cwd=cwd)
        print(command)
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, cwd=cwd)
        output, error = process.communicate()

        output = output.decode('utf-8')
        # print(output)
        output = output.split('\n')
        # print(output)
        result = []
        for i in range(1, len(output) - 1):
            result.append(output[i].split(':')[0])

        return result
    # Method to upload to S3
    def uploadS3(self, s3_client, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name
        try:
            _ = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
        print('Saved {file} to {bucket}'.format(file=file_name, bucket=bucket))

    # Main Method for detection on instance
    def AwsDetection(self):
        flag = True

        while True:
            # First poll SQS for a single message

            filename, receipt = self.receiveMessage(self.queue)
            print(filename)

            if filename is None:
                sqs =  self.queueNumberOfMessages()
                # Check if the number of unread messages is greater than running instances
                if int(sqs['ApproximateNumberOfMessages']) == 0 and int(sqs['ApproximateNumberOfMessagesNotVisible']) == 0:
                    time.sleep(5)
                    break
            wildcard = filename + '_'
            flag = True
            objects = []
            exclude_files = set()
            # Get all file names in the required bucket and filter based on filename
            
            s3 = boto3.resource('s3')
            # for key in s3_client.list_objects(Bucket=self.bucket_im)['Contents']:
            #     print(key)
            s3_client = boto3.client('s3')
            while flag:
                filenames = [key for key in self.tryPaginate(wildcard)
                            if key not in exclude_files]
                # print('filenames', filenames)
                for file in filenames:
                    # Download file
                    self.downloadS3(s3_client, self.bucket_im, file, file)
                    # Delete it from S3
                    # s3.Object(self.bucket_im, file).delete()
                    # Do object detection on the image
                    objects.append(self.objDet(file))
                    # Append to exclude list
                    exclude_files.add(file)
                # Check if no more files exist for request
                if not filenames:
                    flag = False
            objects = [x for y in objects for x in y if x != '']
            objects = list(set(objects))
            objects_st = '\n'.join(objects)
            with open('{x}.txt'.format(x=filename), 'w') as f:
                f.write("%s \n" % objects_st)
            self.uploadS3(s3_client, '{x}.txt'.format(x=filename), self.bucket_res, filename)
            aws.deleteMessage(self.queue, receipt)
            print('Uploaded Results for {v} \n\n'.format(v=filename))
        

    #Get List of File Name Image/Video in S3 with Prefix
    def tryPaginate(self, prefix):
        # print('prefix', prefix)
        client = boto3.client('s3')
        paginator = client.get_paginator('list_objects')
        operation_parameters = {'Bucket': self.bucket_im,
                                'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        response = []
        for page in page_iterator:
            # print(page)
            if 'Contents' in page:
                for item in page['Contents']:
                    response.append(item['Key'])
        # print(response)
        return response



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

    # Get the number of messages in the queue
    def queueNumberOfMessages(self):
        client = boto3.client('sqs')
        response = client.get_queue_attributes(
            QueueUrl=self.queue,
            AttributeNames=[
                'All'
            ]
        )
        return response.get('Attributes')

aws = AWS()
aws.AwsDetection()
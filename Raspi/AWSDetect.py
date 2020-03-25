import boto3
from botocore.exceptions import ClientError
import subprocess

class AWS:
    def __init__(self, queue='https://sqs.us-east-1.amazonaws.com/029834165530/video_queue', bucket_im='raspiimages',
                 bucket_res='raspiresults'):
        self.master = True
        self.queue = queue
        self.bucket_im = bucket_im
        self.bucket_res = bucket_res

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
        cwd = "/home/ubuntu/darknet"
        home = "/home/ubuntu/"
        videopath = home + videopath
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
            result.append(output[i].split(':')[0] + '\n')

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
        # First poll SQS for a single message
        filename, receipt = self.receiveMessage(self.queue)
        aws.deleteMessage(self.queue, receipt)
        print(filename)
        wildcard = filename + '_'
        flag = True
        objects = []
        exclude_files = []
        # Get all file names in the required bucket and filter based on filename
        s3_client = boto3.client('s3')
        s3 = boto3.resource('s3')
        while flag:
            filenames = [key['Key'] for key in s3_client.list_objects(Bucket=self.bucket_im)['Contents']
                         if wildcard in key['Key'] and key['Key'] not in exclude_files]
            for file in filenames:
                # Download file
                self.downloadS3(s3_client, self.bucket_im, file, file)
                # Delete it from S3
                s3.Object(self.bucket_im, file).delete()
                # Do object detection on the image
                objects.append(self.objDet(file))
                # Append to exclude list
                exclude_files.append(file)
            # Check if no more files exist for request
            if not filenames:
                flag = False
        objects = [x for y in objects for x in y if x != '']
        objects = list(set(objects))
        objects_st = ';'.join(objects)
        with open('{x}.txt'.format(x=filename), 'w') as f:
            f.write("%s \n" % objects_st)
        self.uploadS3(s3_client, '{x}.txt'.format(x=filename), self.bucket_res, filename)
        print('Uploaded Results for {v} \n\n'.format(v=filename))



aws = AWS()
aws.AwsDetection()
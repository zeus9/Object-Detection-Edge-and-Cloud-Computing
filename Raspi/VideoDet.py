# Import Statements
import subprocess
import time
import boto3
from botocore.exceptions import ClientError
import re
from picamera import PiCamera
from gpiozero import MotionSensor
import glob
import sys, os
import multiprocessing
from supress_output import suppress_stdout_stderr

class VideoDet:
    def __init__(self, num_of_videos=5, sqs_queue='https://sqs.us-east-1.amazonaws.com/029834165530/image_queue'):
        self.context_flag = False
        self.num_of_videos = num_of_videos
        self.sqs_queue = sqs_queue
    # Disable
    def blockPrint(self):
        sys.stdout = open(os.devnull, 'w')

    # Restore
    def enablePrint(self):
        sys.stdout = sys.__stdout__

    # Main Method to record video motion
    def recordVidoMotion(self):
        camera = PiCamera()
        camera.resolution = (608, 608)
        pir = MotionSensor(4)
        flag = True
        unix_ts = int(time.time())
        count = 1
        motion_flag = False
        video_cnt = 0
        while flag:
            # When Motion is detected
            if pir.motion_detected:
                # Start recording the video when initial motion is detected
                if not motion_flag:
                    camera.start_recording('/home/pi/Raspi/VideoFrames/{v}.h264'.format(v=unix_ts))
                    print('Motion detected, Video has begun recording!')
                filename = '{filename}_{i}.jpeg'.format(filename=unix_ts, i=count)
                filepath = "/home/pi/Raspi/VideoFrames/{filename}".format(filename=filename)
                camera.wait_recording(1)
                camera.capture(filepath, use_video_port=True)
                # When new video is being recorded and the PI is open for processing
                if not self.context_flag and not motion_flag:
                    # Run this in separate thread
                    # Instantiate thread which will perform object detection
                    print('Raspberry PI has begun processing video {v}...'.format(v=unix_ts))
                    thad = multiprocessing.Process(target=self.objDetImg, args=(unix_ts, ))
                    thad.start()
                    # self.objDetImg(unix_ts)

                # When new video is being recorded but PI is not available for processing
                elif self.context_flag and not motion_flag:
                    # Send SQS request to notify that video has to be processed by the cloud
                    print('Raspberry PI busy, request sent to Cloud!')
                    # self.sendMsg(str(unix_ts))
                    try:
                        pool.apply_async(self.sendMsg, args=(str(unix_ts),))
                    except NameError:
                        num_workers = multiprocessing.cpu_count()
                        pool = multiprocessing.Pool(num_workers)
                        pool.apply_async(self.sendMsg, args=(str(unix_ts),))
                    # Once Message has been sent start an instance
                    # try:
                    #     if thad_instance.is_alive():
                    #         pass
                    #     else:
                    #         thad_instance.join()
                    #         thad_instance.start()
                    # except NameError:
                    #     thad_instance = multiprocessing.Process(target=self.instanceManager)
                    #     thad_instance.start()

                # When cloud has to process the video
                if self.context_flag:
                    # print('S3 Here')
                    # Send the images to S3 Bucket
                    pool.apply_async(self.uploadS3, args=(filepath,'raspiimages',filename))

                count = count + 1
                # print('Motion Detected')
                motion_flag = True

            # When motion stops or is not being detected anymore
            else:
                # Flag to indicate if there was previously any motion
                if motion_flag:
                    # Collate all the frames into a single video
                    # command = "ffmpeg -r 1 -i {v}_%01d.jpeg -vcodec mpeg4" \
                    #           " -y /home/pi/Raspi/VideoFrames/{v}.mp4".format(v=unix_ts)
                    cwd = "/home/pi/Raspi/VideoFrames"
                    camera.stop_recording()
                    print('Video {i} has finished recording! \n\n'.format(i=video_cnt))
                    # subprocess.call(command.split(), cwd=cwd)
                    motion_flag = False
                    # Send Video to S3
                    self.uploadS3(cwd + '/' + '{v}.h264'.format(v=unix_ts), 'raspivideos', '{v}.h264'.format(v=unix_ts))
                    video_cnt = video_cnt + 1
                    # End the video recording after a certain number of videos have been recorded
                    if video_cnt >= self.num_of_videos:
                        print('Video Recording Ended \n\n')
                        flag = False
                # Change file name to indicate new video recording
                unix_ts = int(time.time())
                count = 1
                # Check if the thread is still alive, if not then change context to cloud
                try:
                    if thad.is_alive():
                        self.context_flag = True
                    else:
                        thad.join()
                        self.context_flag = False
                except NameError:
                    self.context_flag = False

    # Image object detection
    def objDetImg(self, video):
        cwd = "/home/pi/darknet"
        home = "/home/pi/Raspi/VideoFrames/"
        wildcard = '_*.jpeg'
        videopath = home + str(video) + wildcard
        classfile = open('/home/pi/darknet/data/coco.names')
        classes = classfile.read().split('\n')
        video_op = []
        # self.context_flag = True
        exclude_fname = []
        # See if this loop is functional for files being updated at runtime
        while set(glob.glob(videopath)) != set(exclude_fname):
            for fname in glob.glob(videopath):
                if fname not in exclude_fname:
                    command = "./darknet detector test cfg/coco.data " \
                              "cfg/yolov3-tiny.cfg yolov3-tiny.weights {v} > test.txt".format(v=fname)
                    with suppress_stdout_stderr():
                        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, cwd=cwd)
                        output, error = process.communicate()
                    objects = []
                    for op in str(output).split('\n'):
                        object = [x for x in classes if x in op]
                        objects.append(object)
                    objects = [x for y in objects for x in y if x != '']
                    objects = list(set(objects))
                    video_op.append(objects)
                    exclude_fname.append(fname)
        # print(video_op)
        video_op = [x for y in video_op for x in y if x != '']
        video_op = list(set(video_op))
        video_st = ';'.join(video_op)
        # Send results to S3
        with open(home + '{x}.txt'.format(x=video), 'w') as f:
            f.write("%s \n" % video_st)
        self.uploadS3(home + '{x}.txt'.format(x=video), 'raspiresults', str(video))
        print('\n Uploaded Results for {v} \n'.format(v=video))

    # Method to record video
    def recordVid(self, msecs=3000):
        unix_ts = int(time.time())
        command = "raspivid -o {time}.h264 -t {x}".format(time=unix_ts ,x=msecs)
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        _, _ = process.communicate()
        print("{time}.h264 has been saved".format(time=unix_ts))

        return "{time}.h264".format(time=unix_ts)

    # upload video to S3
    def uploadS3(self, file_name, bucket, object_name=None):
        print('Started sending {f}'.format(f=file_name))
        if object_name is None:
            object_name = file_name
        s3_client = boto3.client('s3')
        try:
            _ = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
        print('Saved {file} to {bucket}'.format(file=file_name, bucket=bucket))

    # SQS send Message
    def sendMsg(self, key):
        sqs = boto3.client('sqs')
        response = sqs.send_message(QueueUrl=self.sqs_queue, MessageBody=key)
        print(response)

    # Run Darknet Object detection
    def objDet(self, video):
        cwd = "/home/pi/darknet"
        home = "/home/pi/Raspi/"
        videopath = home + video
        command = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights test_video.h264".format(v=videopath)
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, cwd=cwd)
        output, error = process.communicate()
        result = re.findall(r'Objects:(.*?)FPS:', str(output))
        classfile = open('/home/pi/darknet/data/coco.names')
        classes = classfile.read().split('\n')
        objects = []
        for res in result:
            object = [x for x in classes if x in res]
            objects.append(object)
        objects = [x for y in objects for x in y if x != '']
        objects = list(set(objects))
        print(objects)

    # Get all stopped instances
    def instanceState(self, instance_name, state):
        # Boto 3
        # Use the filter() method of the instances collection to retrieve
        # all running EC2 instances.
        # ec2 = boto3.client('ec2')
        # response = ec2.describe_instances()
        # print(response)
        ec2 = boto3.resource('ec2')

        instances = ec2.instances.filter(
            # instance-id
            Filters=[{'Name': instance_name, 'Values': state}])
        # for instance in instances:
        #     # print(instance.id, instance.instance_type)
        #     print(instance.id, instance.public_ip_address, instance.state)
        # print(instances)
        return instances

    # Start instances
    def lambda_handler_start(self, event, InstanceIds):
        ec2 = boto3.client('ec2')
        # response = ec2.describe_instances()
        # print(response)
        # instances = ['i-07c17245ca58b0e0f']
        # InstanceIds = []
        # for instance in instances:
        #     if instance.id == 'i-07c17245ca58b0e0f':
        #         InstanceIds.append(instance.id)
        #         break

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
            QueueUrl=self.sqs_queue,
            AttributeNames=[
                'All'
            ]
        )
        return response.get('Attributes')

    # Instance Manager: To check if a new instance must be spawned based on SQS queue
    def instanceManager(self):
        # Check how many instaces are in start and stop state
        instances = self.instanceState('instance-state-name', ['running', 'stopped'])
        # Count the number of running instances
        instance_cnt = 0
        stopped_instances = []
        for instance in instances:
            if instance.state['Name'] == 'running':
                instance_cnt = instance_cnt + 1
            else:
                stopped_instances.append(instance.id)
        print(instance_cnt)
        print(stopped_instances)
        # Now check how many pending SQS messages have yet to be addressed
        response = self.queueNumberOfMessages()
        # Check if the number of unread messages is greater than running instances
        if response['ApproximateNumberOfMessages'] > instance_cnt:
            # Start instances based on difference of messages and instance count
            instance_diff = int(response['ApproximateNumberOfMessages']) - instance_cnt
            self.lambda_handler_start('start', instances[:instance_diff])


vdet = VideoDet()
vdet.recordVidoMotion()

# vdet.instanceManager()

# instances = vdet.instanceState('instance-state-name', ['stopped'])
# InstanceIds = vdet.lambda_handler_start('start', instances)
# vdet.objDetImg('1584245077')

# recordVidoMotion()
# objDetImg('1583981196')
# filename = recordVid()
# sendMsg(filename)
# uploadS3(filename, 'raspivideos')
# objDet(filename)



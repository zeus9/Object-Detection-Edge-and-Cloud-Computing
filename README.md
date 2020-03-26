# Object-Detection-at-the-Edge
An Object Detection System using the Darknet model for Edge Computing on the Raspberry Pi leveraging AWS EC2 Stack

An Edge Computing System with a Raspberry Pi 3 for Object Detection using the Darknet Machine Learning model leveraging AWS EC2 Stack. Briefly, the project detects objects from a live video recording on sensing motion via the IR sensor and scales based on detection request volume.
The Auto-Scaling is built from scratch (without using AWS Cloud Watch) to send the results detected from a video slice to AWS S3 buckets by distributing load between the Pi and multiple AWS EC2 instances (lazily fired). 
This space just holds the modules for Auto-scaling between the edge and cloud load.
The Raspi module is part of the Raspberry Pi 3 and the AWS module is part of the master EC2 instance.

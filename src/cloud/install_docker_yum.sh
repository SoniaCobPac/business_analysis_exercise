#!/bin/bash

echo "***************************"
echo "**DOCKER INSTALLATION STARTED...**"
echo "***************************"

sudo yum update -y
sudo yum uninstall docker -y
sudo amazon-linux-extras install docker -y
sudo service docker start

sudo usermod -a -G docker ec2-user
sudo service docker restart

echo ""
echo "***************************"
echo "** DOCKER INSTALLATION FINISHED **"
echo "***************************"

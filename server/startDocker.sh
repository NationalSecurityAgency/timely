#!/bin/bash


if [ ! -f bcprov-jdk15on-155.jar ];then
	wget https://www.bouncycastle.org/download/bcprov-jdk15on-155.jar
fi

docker-compose build

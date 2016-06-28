FROM centos:centos7

MAINTAINER Jim Klucar "klucar@gmail.com"

VOLUME /timely-server-src

RUN yum -y update && \
  yum -y install java-1.8.0-openjdk-devel maven openssl apr && \
  mkdir -p /timely/bin

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-1.b14.el7_2.x86_64

EXPOSE 9804 54321 54322

ENV TIMELY_VERSION 0.0.2-SNAPSHOT

#CMD /timely/bin/timely-standalone.sh

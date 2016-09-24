FROM centos:centos7

MAINTAINER Jim Klucar "klucar@gmail.com"

VOLUME /timely-server-src
VOLUME /timely

RUN yum -y update && \
  yum -y install java-1.8.0-openjdk-devel maven openssl apr wget && \
  mkdir -p /timely/bin && \
  wget -O /usr/lib/jvm/jre/lib/ext/bcprov-jdk15on-155.jar https://www.bouncycastle.org/download/bcprov-jdk15on-155.jar && \
  echo "security.provider.9=org.bouncycastle.jce.provider.BouncyCastleProvider" >> /usr/lib/jvm/jre/lib/security/java.security

ENV JAVA_HOME /usr/lib/jvm/java

EXPOSE 9804 54321 54322

ENV TIMELY_VERSION 0.0.3-SNAPSHOT


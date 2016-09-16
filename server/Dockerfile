FROM centos:centos7

MAINTAINER Jim Klucar "klucar@gmail.com"

VOLUME /timely-server-src
VOLUME /timely

RUN yum -y update && \
  yum -y install java-1.8.0-openjdk-devel maven openssl apr && \
  mkdir -p /timely/bin

ADD bcprov-jdk15on-155.jar /usr/lib/jvm/jre/lib/ext/

RUN echo security.provider.9=org.bouncycastle.jce.provider.BouncyCastleProvider >> /usr/lib/jvm/jre/lib/security/java.security

ENV JAVA_HOME /usr/lib/jvm/java

EXPOSE 9804 54321 54322

ENV TIMELY_VERSION 0.0.3-SNAPSHOT

#CMD /timely/bin/timely-standalone.sh

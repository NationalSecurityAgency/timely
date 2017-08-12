FROM centos:centos7

VOLUME /timely-server-src
VOLUME /timely

RUN yum -y update && \
  yum -y install java-1.8.0-openjdk-devel maven openssl apr wget epel-release && \
  yum -y install bouncycastle && \
  yum clean all && \
  mkdir -p /timely/bin && \
  echo "security.provider.9=org.bouncycastle.jce.provider.BouncyCastleProvider" >> /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/security/java.security && \
  ln -sf /usr/share/java/bcprov.jar /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/bcprov.jar

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk

EXPOSE 9804 54321 54322

ENV TIMELY_VERSION 0.0.6-SNAPSHOT


FROM openjdk:8-alpine
LABEL mantainer="Salvo Nicotra"
ENV FLUME_VERSION="1.11.0"
ENV PATH /opt/flume/bin:$PATH
ENV HADOOP_VERSION=2.10.1
ENV HADOOP_HOME=/opt/flume/lib/hadoop-$HADOOP_VERSION

RUN apk update && apk add bash

ADD pkg/apache-flume-${FLUME_VERSION}-bin.tar.gz /opt/

# Create sym link
RUN ln -s /opt/apache-flume-${FLUME_VERSION}-bin /opt/flume

RUN wget -q http://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -O /opt/flume/lib/hadoop-$HADOOP_VERSION.tar.gz && \
    tar xzf /opt/flume/lib/hadoop-$HADOOP_VERSION.tar.gz -C /opt/flume/lib && \
    rm /opt/flume/lib/hadoop-$HADOOP_VERSION.tar.gz

RUN mkdir /var/log/netcat
ADD start-flume.sh /opt/flume/bin/start-flume
# Copy All conf here
ADD conf/* /opt/flume/conf/

EXPOSE 44444

ENTRYPOINT [ "start-flume" ]
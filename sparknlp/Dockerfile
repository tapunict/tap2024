FROM python:3.11.3
#RUN apt-get update
#RUN apt-get install -y oracle-java8-installer 
RUN pip install tensorflow==2.16.1 torch==2.3.0
RUN pip3 install pyspark spark-nlp==5.3.3 numpy
RUN apt-get update
RUN apt-get install -y openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

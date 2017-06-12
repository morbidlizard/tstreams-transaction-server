FROM ubuntu:xenial

MAINTAINER Bitworks Software info@bitworks.software

EXPOSE 8071

ENV TTS_VERSION 1.3.7.9-SNAPSHOT
ENV SLF4J_VERSION 1.7.24

ENV DEBIAN_FRONTEND noninteractive

# Install Oracle JDK 8
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list && \
    echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 && \
    apt-get update && \
    apt-get install -y --no-install-recommends apt-utils && \
    echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections && \
    apt-get install -y --no-install-recommends oracle-java8-installer && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN echo "deb http://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 && \
    apt-get update && \
    apt-get install -y --allow-unauthenticated --no-install-recommends sbt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# see .dockerignore in root dir
COPY ./project ./build.sbt ./src ./rundocker.sh /opt/bin/tts/

WORKDIR /opt/bin/tts

RUN mkdir -p /root/.sbt/0.13 && \
    sbt 'set test in assembly := {}' clean assembly && \
    mv target/scala-2.12/tstreams-transaction-server-${TTS_VERSION}.jar . && \
    mv /root/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-${SLF4J_VERSION}.jar . && \
    mv /root/.ivy2/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-${SLF4J_VERSION}.jar . && \
    sbt clean clean-files && \
    rm -rf /root/.ivy2/cache

WORKDIR /var/log/tts
ENTRYPOINT ["/bin/bash", "/opt/bin/tts/rundocker.sh", "/etc/conf/config.properties"]


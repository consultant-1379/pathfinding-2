FROM adoptopenjdk/openjdk11:latest
VOLUME /tmp
WORKDIR /root/parser
ENV PORT 9887
EXPOSE 9887
COPY sbkafka-0.0.1-SNAPSHOT.jar /root/parser/
COPY jmx_prometheus_javaagent-0.14.0.jar /root/parser/
COPY config.yml /root/parser/
ENTRYPOINT exec java -javaagent:/root/parser/jmx_prometheus_javaagent-0.14.0.jar=9679:/root/parser/config.yml -jar sbkafka-0.0.1-SNAPSHOT.jar

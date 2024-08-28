FROM adoptopenjdk/openjdk11:latest
VOLUME /tmp
WORKDIR /root/parser
ENV PORT 9679
EXPOSE 9679
COPY shortlivedjob-0.0.1-SNAPSHOT.jar /root/parser/
COPY jmx_prometheus_javaagent-0.14.0.jar /root/parser/
COPY config.yml /root/parser/
ENTRYPOINT exec java -javaagent:/root/parser/jmx_prometheus_javaagent-0.14.0.jar=9679:/root/parser/config.yml -jar shortlivedjob-0.0.1-SNAPSHOT.jar

FROM adoptopenjdk/openjdk11:latest
VOLUME /tmp
WORKDIR /root/parser
ENV PORT 9887
EXPOSE 9887
# ENV JAVA_OPTS=" -Xmx256m"
ARG JAR_FILE=*.jar
COPY ${JAR_FILE} /root/parser/sbkafka-0.0.1-SNAPSHOT.jar
ENTRYPOINT exec java -XX:+PrintFlagsFinal $JAVA_OPTS -jar sbkafka-0.0.1-SNAPSHOT.jar

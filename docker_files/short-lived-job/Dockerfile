FROM adoptopenjdk/openjdk11:latest
VOLUME /tmp
WORKDIR /root/parser
ENV PORT 8889
EXPOSE 8889
# ENV JAVA_OPTS=" -Xmx256m"
ARG JAR_FILE=*.jar
COPY ${JAR_FILE} /root/parser/shortlivedjob-0.0.1-SNAPSHOT.jar
ENTRYPOINT exec java -XX:+PrintFlagsFinal $JAVA_OPTS -jar shortlivedjob-0.0.1-SNAPSHOT.jar

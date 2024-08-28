FROM adoptopenjdk/openjdk11:latest
VOLUME /tmp
WORKDIR /root/controller
ENV PORT 8009
EXPOSE 8009
# ENV JAVA_OPTS=" -Xmx256m"
ARG JAR_FILE=*.jar
COPY ${JAR_FILE} /root/controller/parsercontroller-0.0.1-SNAPSHOT.jar
ENTRYPOINT exec java -XX:+PrintFlagsFinal $JAVA_OPTS -jar parsercontroller-0.0.1-SNAPSHOT.jar

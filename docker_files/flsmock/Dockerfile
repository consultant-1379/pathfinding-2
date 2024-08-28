FROM adoptopenjdk/openjdk11:latest
VOLUME /tmp
WORKDIR /root/fls
ENV PORT 9777
EXPOSE 9777
ARG JAR_FILE=*.jar
COPY ${JAR_FILE} /root/fls/flsmock-0.0.1-SNAPSHOT.jar
ENTRYPOINT exec java $JAVA_OPTS -jar flsmock-0.0.1-SNAPSHOT.jar
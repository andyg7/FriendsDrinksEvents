FROM gradle:jdk8 as builder

COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build
RUN gradle shadowJar

FROM openjdk:8-jre-slim

RUN mkdir /app

COPY --from=builder /home/gradle/src/build/libs/*.jar /app
COPY config/app/dev.properties /app

CMD ["sleep", "10000"]

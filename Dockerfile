FROM gradle:6.8.0-jdk8 as builder

COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build
RUN gradle shadowJar

FROM openjdk:8-jre-slim

RUN mkdir /app

COPY --from=builder /home/gradle/src/build/libs/*.jar /app
COPY config/app/dev.properties /app
COPY config/app/dev-kubernetes.properties /app

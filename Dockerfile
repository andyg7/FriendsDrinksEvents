FROM openjdk:8-jre-slim

RUN mkdir /app
RUN mkdir -p /app/var/output

COPY build/libs/friendsdrinks-0.0.1.jar /app
COPY config/app/dev.properties /app


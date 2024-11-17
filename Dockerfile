FROM gradle:jdk17-corretto-al2023 as builder
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build
RUN gradle shadowJar

FROM openjdk:17
RUN mkdir -p /app/config
COPY --from=builder /home/gradle/src/build/libs/*.jar /app
ENTRYPOINT ["java", "-cp", "/app/friendsdrinks-0.0.1.jar"]

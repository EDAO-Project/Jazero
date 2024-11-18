FROM ubuntu:20.04

WORKDIR /home

RUN apt-get update
RUN apt-get install openjdk-17-jre openjdk-17-jdk maven -y

ARG MVNW=mvnw_unix

ADD mvnw .
RUN sed -e 's/\r$//' mvnw > mvnw_unix
RUN chmod +x mvnw_unix

RUN mkdir web/
ADD .mvn/ .mvn/
ADD dependencies/ dependencies/
ADD config.json .
ADD pom.xml .
ADD src/ src/

RUN mvn package -Pproduction

ENTRYPOINT ["./mvnw_unix", "spring-boot:run"]
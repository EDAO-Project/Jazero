FROM ubuntu

RUN apt update
RUN apt install openjdk-17-jre openjdk-17-jdk maven -y

WORKDIR /home
COPY .. .
WORKDIR communication
RUN mvn clean install

ARG MVNW=mvnw_unix

WORKDIR ../data-lake
RUN sed -e 's/\r$//' mvnw > mvnw_unix
RUN chmod +x mvnw_unix

ENTRYPOINT ["./mvnw_unix", "spring-boot:run"]
FROM ubuntu

RUN mkdir /index
RUN apt-get update
RUN apt-get install openjdk-17-jre openjdk-17-jdk maven -y

WORKDIR /home

RUN mkdir communication
RUN mkdir storage
RUN mkdir data-lake
COPY communication communication
COPY storage storage
COPY data-lake data-lake

WORKDIR communication
RUN mvn clean install -DskipTests

WORKDIR ../storage
RUN mvn clean install -DskipTests

ARG MVNW=mvnw_unix

WORKDIR ../data-lake
RUN sed -e 's/\r$//' mvnw > mvnw_unix
RUN chmod +x mvnw_unix

ENTRYPOINT ["./mvnw_unix", "spring-boot:run"]

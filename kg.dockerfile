FROM ubuntu

WORKDIR /home
COPY *.ttl ./kg.ttl

RUN apt update
RUN apt install wget default-jdk -y

ENTRYPOINT ["./install", ".", "&&", "mkdir", "-p", "kg", "&&", "mv", "kg.ttl", "kg",
            "&&", "./import.sh", "kg/kg.ttl", "neo4j-server"]
FROM ubuntu

RUN apt-get update
RUN apt install wget openjdk-11-jdk -y

WORKDIR /home
COPY *.ttl ./kg.ttl

ENTRYPOINT ["./install.sh", ".", "&&", "mkdir", "-p", "kg", "&&", "mv", "kg.ttl", "kg", "&&", \
            "./import.sh", "kg/kg.ttl", "neo4j-server"]

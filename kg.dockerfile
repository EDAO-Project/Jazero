FROM ubuntu

WORKDIR /srv
ARG USER

RUN apt-get update
RUN apt install wget openjdk-11-jdk -y

CMD echo "Pass a command to run"

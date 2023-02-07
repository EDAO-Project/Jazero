FROM ubuntu

WORKDIR /home

RUN apt update
RUN apt install python3 pip git -y
RUN python3 -m pip install Django tzdata requests

COPY api/jdlc_web/ jdlc_web/
COPY jdlc/jdlc.py jdlc_web/jdlc/

WORKDIR jdlc_web/

EXPOSE 8084
ENTRYPOINT python3 manage.py runserver 0.0.0.0:8084
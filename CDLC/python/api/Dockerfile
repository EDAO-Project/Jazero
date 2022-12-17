FROM ubuntu

WORKDIR /home

RUN apt update
RUN apt install python3 pip git -y
RUN python3 -m pip install Django
RUN django-admin startproject mysite

COPY cdlc_web/ cdlc_web/
RUN git clone https://github.com/EDAO-Project/Calypso.git
RUN mv Calypso/CDLC/python/cdlc.py cdlc_web/cdlc

WORKDIR cdlc_web/mysite

EXPOSE 8084
ENTRYPOINT python3 manage.py runserver 0.0.0.0:8084
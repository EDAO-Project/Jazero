FROM ubuntu

WORKDIR /home

RUN apt update
RUN apt install python3 pip -y
RUN python3 -m pip install Django
RUN django-admin startproject mysite

WORKDIR mysite
ENTRYPOINT python3 manage.py runserver 0.0.0.0:8084
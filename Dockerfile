FROM python:3.7-slim

RUN apt-get clean \
    && apt-get -y update

RUN apt-get -y install python3-dev \
    && apt-get -y install build-essential 

WORKDIR /app

COPY requirements.txt  /app
RUN pip install -r requirements.txt --src /usr/local/src

COPY pagai /app/pagai
COPY start.sh /app
COPY uwsgi.ini /app

ENV JOBLIB_MULTIPROCESSING=0

CMD ["./start.sh"] 

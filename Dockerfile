FROM python:3.7-slim

RUN apt-get clean \
    && apt-get -y update
    
RUN apt-get -y install python3-dev \
    && apt-get -y install build-essential 

WORKDIR /srv/pagai

COPY requirements.txt  /srv/pagai/requirements.txt
RUN pip install -r requirements.txt --src /usr/local/src

COPY . /srv/pagai

RUN chmod +x ./start.sh
CMD ["./start.sh"] 

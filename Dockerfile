FROM arkhn/python-base:latest

WORKDIR /app

COPY requirements.txt  /app
RUN pip install -r requirements.txt --src /usr/local/src

COPY pagai /app/pagai
COPY start.sh /app
COPY uwsgi.ini /app

ENV JOBLIB_MULTIPROCESSING=0

CMD ["./start.sh"] 

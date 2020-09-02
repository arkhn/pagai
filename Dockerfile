FROM arkhn/python-base:all

WORKDIR /app

COPY requirements/requirements-base.txt  /app/requirements-base.txt
COPY requirements/requirements-all.txt  /app/requirements.txt
RUN pip install -r requirements.txt --src /usr/local/src

COPY pagai /app/pagai
COPY start.sh /app
COPY uwsgi.ini /app

ENV JOBLIB_MULTIPROCESSING=0

CMD ["./start.sh"] 

FROM python:slim-buster

WORKDIR app

COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY consumer.py ./

CMD ["python", "consumer.py"]

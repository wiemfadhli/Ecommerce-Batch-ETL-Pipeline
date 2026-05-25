FROM python:3.11

WORKDIR /opt/producer
COPY producer.py ./

RUN pip install kafka-python

CMD ["python", "producer.py"]



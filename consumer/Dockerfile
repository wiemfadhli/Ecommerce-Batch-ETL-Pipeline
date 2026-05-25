FROM python:3.11

WORKDIR /app
COPY consumer.py .

RUN pip install kafka-python

CMD ["python", "consumer.py"]

FROM python:3.10-slim

WORKDIR /app
COPY transformer.py .

RUN pip install kafka-python psycopg2-binary

CMD ["python", "transformer.py"]




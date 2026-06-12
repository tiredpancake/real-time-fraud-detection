FROM python:3.10-slim

WORKDIR /app

COPY producer.py .
COPY creditcard.csv .

RUN pip install kafka-python pandas

CMD ["python", "producer.py"]

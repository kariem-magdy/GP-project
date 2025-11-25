FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY logsGenerator/ /app/logsGenerator/

WORKDIR /app/logsGenerator

CMD ["python", "main_producers.py"]

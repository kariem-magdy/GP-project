FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire folder structure
COPY logsGenerator/ ./logsGenerator/

# Set unbuffered output so logs show up immediately in Docker
ENV PYTHONUNBUFFERED=1

# Run the script
CMD ["python", "logsGenerator/kafka_producers.py"]
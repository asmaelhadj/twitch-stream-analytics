FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY twitch_chat_producer.py .

CMD ["python", "twitch_chat_producer.py"]
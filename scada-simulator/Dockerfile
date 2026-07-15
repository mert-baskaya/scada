FROM python:3.12-slim

RUN pip install --no-cache-dir kafka-python-ng

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY simulator.py .

CMD ["python", "-u", "simulator.py"]

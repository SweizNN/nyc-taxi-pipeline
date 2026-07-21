FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Build context'i `./producer` olduğu için, bu klasördeki her şeyi kopyala
COPY . .
CMD ["python", "main.py"]

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dob_permits_poller.py .

CMD ["python", "dob_permits_poller.py"]

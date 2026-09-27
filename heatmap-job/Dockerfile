FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY make_heatmaps.py .
CMD ["python", "make_heatmaps.py"]

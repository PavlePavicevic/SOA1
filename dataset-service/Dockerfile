FROM python:3.11-slim

WORKDIR /app
COPY Requirements.txt .
RUN pip install --no-cache-dir -r Requirements.txt

COPY . .
EXPOSE 5001
CMD ["python", "app.py"]
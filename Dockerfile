FROM python:3.12.9-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r /app/src/requirements.txt
EXPOSE 80
ENV NAME World
CMD ["python", "src/main.py"]

FROM python:3.12-slim
WORKDIR /app
COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto /proto
RUN python -m grpc_tools.protoc -I/proto --python_out=/app /proto/chat.proto
COPY python/app.py .
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "app.py"]

# Imagem enxuta com runtime Python para bots, servidores, broker e serviços auxiliares.
FROM python:3.12-slim
# Mantém caminhos relativos previsíveis dentro do container.
WORKDIR /app
# Instala somente as bibliotecas usadas pelo cliente ZeroMQ e pelo Protobuf.
COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# O contrato é compilado durante a imagem para gerar a classe Python do envelope.
COPY proto /proto
RUN python -m grpc_tools.protoc -I/proto --python_out=/app /proto/chat.proto
# Copia os processos da aplicação sem incluir arquivos locais de build.
COPY python/*.py .
# Faz os logs aparecerem imediatamente no docker compose logs.
ENV PYTHONUNBUFFERED=1
# O comando do Compose seleciona o papel: bot, server, broker, pubsub ou reference.
ENTRYPOINT ["python", "app.py"]


# Validação — Parte 3

Os testes usam sockets ZeroMQ reais, serialização Protobuf e bancos SQLite
temporários. Não utilizam mocks dos servidores.

## Como reproduzir

Depois de instalar as dependências Python, gerar chat_pb2.py e compilar o Java
com Maven (comandos no README):

```bash
python tests/integration.py
python tests/parte3.py
```

`integration.py` verifica login, canais, erros, deduplicação, publicações,
persistência após reiniciar, bots Python com servidor Java e bots Java com
servidor Python. Também inicia a topologia de onze processos: referência,
broker, proxy, quatro servidores e quatro bots, com quatro bancos separados.

`parte3.py` verifica cadastro sem duplicação, ranks distintos, listagem,
expiração, reativação preservando rank, contador lógico nas respostas,
sincronização física, heartbeat no décimo recebimento, retransmissões com novos
contadores e rejeição de reutilização de ID com conteúdo diferente.

## Ambiente e limites

Validação local em Linux com Python 3.12 e runtime Java 21; fontes Java
compiladas para nível Java 17 com Eclipse ECJ 3.39.0 e Protobuf 3.25.5,
usando as versões de bibliotecas declaradas no pom.xml.
A compilação Maven neste ambiente ficou bloqueada na resolução de plugins;
por isso foi usada compilação direta para validar o código Java.
Docker não está instalado neste ambiente. O build das imagens e a execução
via Docker Compose precisam ser confirmados no computador da dupla:

```bash
docker compose up --build
```

A ausência de tráfego por mais de 60 segundos também expira um servidor, pois
o heartbeat segue o critério por quantidade de mensagens exigido no enunciado.
Os ranks são estáveis durante a execução da referência, não entre reinícios dela.

## Resultado da execução em 10/11/2026

```text
PASS servidor python: erros, deduplicação, persistência e Pub/Sub com bot java
PASS servidor java: erros, deduplicação, persistência e Pub/Sub com bot python
PASS integração completa
PASS topologia de 11 processos: referência, broker, proxy, 4 bots, 4 servidores e 4 bancos separados
PASS referência: cadastro único, ranks, listagem, expiração e retorno
PASS python: relógio lógico, sincronização física, heartbeat após 10 mensagens, retry e remoção
PASS java: relógio lógico, sincronização física, heartbeat após 10 mensagens, retry e remoção
PASS Parte 3 completa
PASS Compose: 11 serviços, dependências e Dockerfiles (validação estática YAML)
```

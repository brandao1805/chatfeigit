# Validação da Parte 5

Os testes usam processos Python e Java reais, sockets ZeroMQ, Protobuf e bancos
SQLite temporários separados. A suíte parte5.py compara todas as linhas das
quatro tabelas em quatro réplicas, com 160 publicações para exigir paginação.
Depois encerra um servidor Java, produz mais vinte mensagens, remove apenas
o banco temporário desse servidor e inicia novamente, verificando recuperação
e consulta paginada do histórico completo em todos os servidores.

A suíte parte4.py verifica eleição, Berkeley e troca de coordenador com a
replicação habilitada. Para reproduzir, siga a preparação local do README.

Java compilado para nível Java 17 com Eclipse ECJ 3.39.0, Protobuf 3.25.5 e
as dependências declaradas no pom.xml. A execução local usa o runtime Java
instalado.

## Compose

`docker compose config --quiet` passou com Docker Compose v5.6.1. A execução
dos containers não pôde ser validada neste ambiente: `docker compose up
--build -d` não conseguiu conectar ao socket
`unix:///Users/brandao/.docker/run/docker.sock`, e o aplicativo Docker Desktop
não está instalado. Portanto, os resultados abaixo validam os processos e a
configuração Compose, mas não substituem uma execução completa de
`docker compose up --build` em uma máquina com o daemon Docker ativo.

Para concluir essa checagem na máquina de apresentação:

```bash
docker compose up --build -d
docker compose ps
docker compose logs --since=2m python-server-1 java-server-1 python-bot-1 java-bot-1
docker compose down
```

Nos logs, confira os bots fazendo login e publicando, mensagens `SUB RECV`,
registros `HEARTBEAT`, eleição e sincronização Berkeley. `docker compose ps`
deve mostrar os onze serviços em execução antes do encerramento.

## Ajuste da eleição

Uma coleta de logs do Compose mostrou servidores alternando entre coordenadores
diferentes durante respostas atrasadas. A detecção foi ajustada nas duas
implementações: uma falha isolada de sonda não remove o servidor; são exigidas
duas falhas consecutivas, com timeout de 1,5 segundo por tentativa. A sintaxe
Python foi verificada e `mvn -DskipTests package` compilou a implementação
Java. A execução Docker precisa ser repetida após este ajuste. Depois da
inicialização, observe por pelo menos um minuto se os servidores convergem
para o mesmo nome em suas linhas `ELECTED`.

## Resultados

```text
PASS quatro réplicas iguais: logins, canais, requisições e 160 publicações paginadas
PASS consulta paginada do histórico completo nas quatro réplicas
PASS recuperação de banco vazio e mensagens produzidas durante a indisponibilidade
PASS eleição: Java (maior rank) coordenador, Python seguidor
PASS Berkeley: coleta TIME, média, ADJUST e anúncio Pub/Sub entre linguagens
PASS falha: Python assume após desligamento do coordenador Java
PASS sintaxe Python e estrutura YAML com onze serviços
```

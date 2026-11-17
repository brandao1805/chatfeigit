# ChatFEI — Parte 1

Sistema de chat distribuído com clientes e servidores em Python e Java. Esta versão implementa login, criação e listagem de canais, comunicação Request-Reply e persistência local em SQLite.

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — cliente e servidor Python.
- Yuri Lucas Oishi | 22.225.025-0 — cliente e servidor Java.

## Tecnologias

- Python e PyZMQ.
- Java e JeroMQ.
- Protocol Buffers para serialização.
- SQLite para persistência.
- Docker e Docker Compose para execução dos serviços.

## Execução

Com o Docker Desktop iniciado, abra um terminal na pasta que contém o arquivo `docker-compose.yaml` e execute:

```bash
docker compose up --build
```

O Compose inicia nove containers:

- Dois bots Python.
- Dois bots Java.
- Dois servidores Python.
- Dois servidores Java.
- Um broker Request-Reply.

A primeira execução precisa de acesso à internet para baixar imagens e dependências. A execução pelo Docker não exige a instalação local de Python, Java, Maven ou Protobuf.

Os bots fazem login, criam o canal `geral` e um canal próprio e listam os canais a cada três segundos.

Para acompanhar os servidores:

```bash
docker compose logs -f python-server-1 java-server-1
```

Para encerrar os serviços:

```bash
docker compose down
```

Para iniciar novamente:

```bash
docker compose up
```

O comando `docker compose down` preserva os volumes SQLite. Para encerrar os serviços e apagar os dados persistidos:

```bash
docker compose down -v
```

## Arquitetura

Os bots utilizam sockets REQ e se conectam ao socket ROUTER do broker na porta 5555. O broker encaminha as requisições pelo socket DEALER, na porta 5556, aos servidores REP.

As requisições são distribuídas entre as conexões disponíveis. O broker preserva os frames de roteamento e não interpreta o conteúdo das mensagens. As portas são internas à rede do Compose.

Python utiliza PyZMQ e Java utiliza JeroMQ. As duas implementações compartilham o contrato definido em `proto/chat.proto`.

O Protobuf realiza a serialização e o ZeroMQ realiza o transporte. Não há comunicação gRPC.

Cada servidor mantém um arquivo `/data/chat.db` em um volume Docker exclusivo. Não existe banco compartilhado entre os servidores.

Os clientes são bots automáticos, sem interface gráfica.

## Contrato de comunicação

Cada requisição contém:

- `request_id`: identificador UUID.
- `timestamp_ms`: horário Unix UTC em milissegundos.
- `operation`: operação solicitada.
- `username`: nome do usuário.
- `channel`: nome do canal, quando necessário.

As respostas contêm o mesmo identificador da requisição, timestamp próprio, operação, status, descrição e identificação do servidor. A listagem também contém o campo `channels`.

| Operação | Entrada adicional | Resultado |
|---|---|---|
| LOGIN | Nenhuma | Registra usuário e timestamp |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | Nenhuma | Lista ordenada dos canais locais |

Nomes de usuários e canais diferenciam maiúsculas de minúsculas e aceitam de 1 a 32 caracteres ASCII: letras, números, `_` e `-`.

O login registra a entrada do usuário, sem senha ou sessão de autenticação. Cada operação informa o usuário, permitindo que requisições seguintes sejam atendidas por outro servidor.

## Tratamento de erros e retransmissões

Os principais códigos de erro são:

- INVALID_REQUEST.
- INVALID_USER.
- INVALID_CHANNEL.
- UNKNOWN_OPERATION.
- INVALID_PROTOBUF.
- STORAGE_ERROR.
- ID_CONFLICT.

O bot repete o login em caso de erro. Quando ocorre timeout de três segundos, fecha o socket REQ, cria outro e retransmite a operação com o mesmo identificador e timestamp.

O timestamp da requisição representa o envio inicial. Cada resposta recebe um timestamp atualizado.

O servidor grava a requisição, a resposta e seus efeitos na mesma transação SQLite.

Uma requisição repetida com o mesmo identificador e conteúdo retorna a resposta armazenada, sem repetir o efeito no mesmo servidor. A reutilização do identificador com outro conteúdo retorna `ID_CONFLICT`.

Essa deduplicação é local. Uma retransmissão atendida por outro servidor pode ser registrada novamente, sem garantia global de processamento exatamente uma vez.

## Persistência

Cada banco possui as tabelas:

| Tabela | Conteúdo |
|---|---|
| logins | Identificador, usuário e timestamp |
| channels | Nome, criador e timestamp |
| requests | Identificador e envelopes binários da requisição e resposta |

As consultas utilizam parâmetros SQL. Os registros permanecem disponíveis após o reinício do container quando o volume é preservado.

## Limitações

- Cada servidor conhece somente seus próprios registros.
- A listagem pode retornar canais diferentes conforme o servidor que atende a requisição.
- `ALREADY_EXISTS` considera apenas o banco local.
- Não há publicação de mensagens, replicação, eleição de coordenador ou sincronização de relógios nesta versão.
- O broker é um ponto único de falha.
- Não há autenticação por senha nem controle de acesso.

## Desenvolvimento local

Requisitos: Python 3.12, Java 17 e Maven.

Preparação do Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r python/requirements.txt
python -m grpc_tools.protoc -Iproto --python_out=python proto/chat.proto
```

Compilação do Java:

```bash
mvn -f java/pom.xml package
```

## Testes de integração

Após preparar as dependências e compilar o Java:

```bash
python tests/integration.py
```

Os testes utilizam portas e bancos temporários e encerram os processos ao finalizar. Os cenários incluem interoperabilidade entre Python e Java, login, criação e listagem de canais, entradas inválidas, deduplicação e persistência após reinício.

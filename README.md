# ChatFEI — Parte 2

Sistema de chat distribuído com clientes e servidores em Python e Java. Implementa login, criação e listagem de canais, publicação de mensagens e inscrição em canais, combinando Request-Reply e Publish-Subscribe.

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — cliente e servidor Python.
- Yuri Lucas Oishi | 22.225.025-0 — cliente e servidor Java.

## Tecnologias

- Python e PyZMQ.
- Java e JeroMQ.
- Protocol Buffers para serialização.
- SQLite para persistência.
- Docker e Docker Compose para execução dos serviços.

## Justificativa das escolhas

Python e Java permitem implementar clientes e servidores em linguagens diferentes e verificar sua interoperabilidade. PyZMQ e JeroMQ oferecem suporte aos padrões de comunicação utilizados pelo sistema.

Protocol Buffers foi escolhido pela serialização binária e pela geração de classes para as duas linguagens a partir de um contrato comum.

SQLite foi escolhido por permitir persistência em disco e transações sem exigir um serviço de banco separado. Cada servidor mantém um arquivo próprio em volume exclusivo.

Docker Compose reúne os serviços e sua rede em uma configuração única, permitindo iniciar clientes, servidores e componentes auxiliares com um único comando.

Request-Reply permite confirmar operações e informar erros ao bot. Publish-Subscribe distribui as publicações aos clientes inscritos nos canais. O armazenamento das publicações preserva identificador, canal, autor, timestamp e conteúdo para consultas posteriores.

Ao receber uma publicação, o bot exibe o canal, o conteúdo, o timestamp de envio e o timestamp de recebimento.

## Execução

Com o Docker Desktop iniciado, abra um terminal na pasta que contém `docker-compose.yaml`:

```bash
docker compose up
```

Para reconstruir as imagens após alterações no código ou nas dependências, utilize `docker compose up --build`.

O Compose inicia dez containers:

- Dois bots Python.
- Dois bots Java.
- Dois servidores Python.
- Dois servidores Java.
- Um broker Request-Reply.
- Um proxy Publish-Subscribe.

A primeira execução baixa imagens e dependências. Não é necessário instalar Python, Java, Maven ou Protobuf localmente para executar pelo Docker.

Os bots fazem login e consultam os canais. Enquanto conhecem menos de cinco canais, tentam criar um novo. A cada rodada, inscrevem-se em mais um canal aleatório, até três inscrições. Depois escolhem um canal conhecido e enviam dez mensagens de conteúdo aleatório, com intervalo de um segundo entre os envios.

Para acompanhar os bots:

```bash
docker compose logs -f python-bot-1 java-bot-1
```

Os registros `SUB inscrito em`, `PUBLISH` com status `OK` e `SUB RECV` com operação `PUBLICATION` indicam inscrição, publicação e recebimento.

Para encerrar:

```bash
docker compose down
```

Os volumes são preservados. Para apagar também os dados persistidos:

```bash
docker compose down -v
```

## Arquitetura

### Request-Reply

Os bots REQ se conectam ao ROUTER do broker na porta 5555. O DEALER do broker, na porta 5556, distribui as requisições aos servidores REP.

O broker preserva os frames de roteamento e não interpreta o conteúdo das mensagens.

### Publish-Subscribe

O bot solicita uma publicação por Request-Reply. O servidor grava a mensagem no SQLite, envia a publicação pelo socket PUB e responde ao bot.

Os servidores PUB se conectam ao XSUB do proxy em `pubsub:5557`. Os bots SUB se conectam ao XPUB em `pubsub:5558`.

O proxy encaminha publicações e inscrições. Cada publicação possui dois frames:

1. Nome do canal.
2. Envelope Protobuf com operação `PUBLICATION`.

Os bots verificam a igualdade do nome do canal, pois o filtro SUB do ZeroMQ utiliza correspondência por prefixo.

As portas são internas à rede Compose.

### Comunicação entre linguagens

Python utiliza PyZMQ e Java utiliza JeroMQ. O contrato está em `proto/chat.proto`, a partir do qual são geradas as classes das duas linguagens.

O Protobuf serializa as mensagens e o ZeroMQ realiza o transporte. Não há comunicação gRPC.

## Contrato de comunicação

Cada requisição contém `request_id`, `timestamp_ms`, `operation` e `username`. Os campos `channel` e `body` são utilizados conforme a operação.

As respostas incluem o identificador da requisição, timestamp próprio, operação, status, descrição e `server_id`.

| Operação | Entrada adicional | Resultado |
|---|---|---|
| LOGIN | Nenhuma | Registra a entrada do usuário |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | Nenhuma | Lista ordenada dos canais locais |
| PUBLISH | channel e body | OK, INVALID_CHANNEL ou INVALID_BODY |

Nomes de usuários e canais diferenciam maiúsculas de minúsculas e aceitam de 1 a 32 caracteres ASCII: letras, números, `_` e `-`.

O login não utiliza senha nem mantém sessão de autenticação. Cada operação informa o usuário.

O conteúdo da publicação deve ser não vazio e ter até 4096 bytes em UTF-8.

Como o broker pode encaminhar uma publicação para um servidor que ainda não conhece o canal, `PUBLISH` registra localmente um nome de canal válido quando necessário.

## Publicações e recebimento

O status `OK` confirma o registro local e o encaminhamento da publicação pelo socket PUB. Não confirma o recebimento por todos os assinantes.

Um assinante recebe mensagens enquanto está conectado e inscrito. Publicações anteriores à inscrição ou perdidas durante uma desconexão permanecem no banco do servidor que as gravou, mas não são reenviadas automaticamente pelo Pub/Sub.

Nos logs:

- `timestamp_ms` representa o horário de envio da publicação pelo servidor.
- `received_at_ms` registra o horário de recebimento pelo bot.
- `server_id` identifica o servidor responsável.

## Erros e retransmissões

Os códigos de erro incluem `INVALID_BODY`, `INVALID_REQUEST`, `INVALID_USER`, `INVALID_CHANNEL`, `UNKNOWN_OPERATION`, `INVALID_PROTOBUF`, `STORAGE_ERROR` e `ID_CONFLICT`.

Em timeout de três segundos, o bot recria o socket REQ e repete a operação com o mesmo identificador e timestamp.

O servidor salva requisição, resposta e efeitos na mesma transação. A repetição do mesmo identificador e conteúdo no mesmo servidor retorna a resposta armazenada. Outro conteúdo com o mesmo identificador retorna `ID_CONFLICT`.

A deduplicação é local, sem garantia global de processamento exatamente uma vez.

## Persistência

Cada servidor possui um arquivo `/data/chat.db` em volume Docker exclusivo.

| Tabela | Conteúdo |
|---|---|
| logins | Identificador, usuário e timestamp |
| channels | Nome, criador e timestamp |
| requests | Identificador e envelopes binários da requisição e resposta |
| publications | Identificador, canal, autor, horário e texto |

As consultas utilizam parâmetros SQL. Os bancos não são compartilhados.

## Limitações

- Não há replicação entre os bancos.
- A listagem de canais depende do servidor que atende a requisição.
- `ALREADY_EXISTS` considera somente os registros locais.
- Não há relógio lógico, serviço de referência, eleição ou sincronização Berkeley.
- Broker e proxy são componentes únicos.
- O Pub/Sub não recupera automaticamente mensagens perdidas.
- Não há autenticação por senha nem controle de acesso.

## Desenvolvimento local

Requisitos: Python 3.12, Java 17 e Maven.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r python/requirements.txt
python -m grpc_tools.protoc -Iproto --python_out=python proto/chat.proto
mvn -f java/pom.xml package
```

## Testes de integração

```bash
python tests/integration.py
```

Os testes utilizam processos, portas e bancos temporários. Verificam operações de usuários e canais, publicação entre Python e Java, validação de mensagens, deduplicação e persistência.

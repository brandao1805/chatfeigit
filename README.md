# ChatFEI — Parte 3

Sistema de chat distribuído em Python e Java com Request-Reply, Publish-Subscribe, persistência SQLite, relógios lógicos, serviço de referência e heartbeat.

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — cliente e servidor Python.
- Yuri Lucas Oishi | 22.225.025-0 — cliente e servidor Java.

## Tecnologias

- Python e PyZMQ.
- Java e JeroMQ.
- Protocol Buffers.
- SQLite.
- Docker e Docker Compose.

## Justificativa das escolhas

Python e Java permitem implementar clientes e servidores em linguagens diferentes e verificar sua interoperabilidade. PyZMQ e JeroMQ oferecem suporte aos padrões de comunicação utilizados pelo sistema.

Protocol Buffers foi escolhido pela serialização binária e pela geração de classes para as duas linguagens a partir de um contrato comum.

SQLite foi escolhido por permitir persistência em disco e transações sem exigir um serviço de banco separado. Cada servidor mantém um arquivo próprio em volume exclusivo.

Docker Compose reúne os serviços e sua rede em uma configuração única, permitindo iniciar clientes, servidores e componentes auxiliares com um único comando.

Request-Reply permite confirmar operações e informar erros ao bot. Publish-Subscribe distribui as publicações aos clientes inscritos nos canais. O armazenamento das publicações preserva identificador, canal, autor, timestamp e conteúdo para consultas posteriores.

Ao receber uma publicação, o bot exibe o canal, o conteúdo, o timestamp de envio e o timestamp de recebimento.

## Execução

Com o Docker Desktop iniciado, execute na pasta que contém `docker-compose.yaml`:

```bash
docker compose up
```

Para reconstruir as imagens após alterações no código ou nas dependências, utilize `docker compose up --build`.

O Compose inicia onze containers:

- Dois bots Python.
- Dois bots Java.
- Dois servidores Python.
- Dois servidores Java.
- Um broker Request-Reply.
- Um proxy Publish-Subscribe.
- Um serviço de referência.

A primeira execução baixa imagens e dependências. A execução pelo Docker não exige a instalação local das ferramentas de desenvolvimento.

Os bots fazem login e descobrem canais. Enquanto conhecem menos de cinco canais, tentam criar um novo. A cada rodada, inscrevem-se em mais um canal aleatório, até três inscrições, e enviam dez mensagens para um canal conhecido, com intervalo de um segundo.

Para acompanhar os bots:

```bash
docker compose logs -f python-bot-1 java-bot-1
```

Para acompanhar referência e servidores:

```bash
docker compose logs -f reference python-server-1 java-server-1
```

Para encerrar:

```bash
docker compose down
```

Os volumes SQLite são preservados. Para apagar também os dados:

```bash
docker compose down -v
```

## Arquitetura

Os bots REQ se conectam ao ROUTER do broker na porta 5555. O DEALER, na porta 5556, encaminha as requisições aos servidores REP.

As publicações são solicitadas por Request-Reply. O servidor grava a mensagem no SQLite, publica pelo socket PUB e responde ao bot.

Os servidores PUB utilizam o XSUB do proxy em `pubsub:5557`. Os bots SUB utilizam o XPUB em `pubsub:5558`.

Cada publicação contém o canal no primeiro frame e um envelope Protobuf com operação `PUBLICATION` no segundo. Os bots verificam a igualdade do canal, pois o filtro nativo do ZeroMQ utiliza prefixos.

O serviço `reference` atende aos servidores por REQ/REP na porta 5559.

As portas são internas à rede Compose. Broker e proxy encaminham os frames sem modificar os envelopes.

Python utiliza PyZMQ e Java utiliza JeroMQ. O contrato compartilhado está em `proto/chat.proto`. Protobuf realiza a serialização e ZeroMQ realiza o transporte, sem gRPC.

## Operações dos usuários

As requisições contêm identificador UUID, timestamp, relógio lógico, operação e usuário. Canal e conteúdo são informados conforme a operação.

| Operação | Entrada adicional | Resultado |
|---|---|---|
| LOGIN | Nenhuma | Registra a entrada do usuário |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | Nenhuma | Lista dos canais locais |
| PUBLISH | channel e body | Registra e publica a mensagem |

Nomes de usuários e canais diferenciam maiúsculas de minúsculas e aceitam de 1 a 32 caracteres ASCII: letras, números, `_` e `-`.

O login não utiliza senha. Cada operação informa o usuário, sem depender de uma sessão no servidor.

Publicações devem conter texto e aceitar no máximo 4096 bytes UTF-8. Um canal válido ainda desconhecido pelo servidor é registrado localmente ao receber `PUBLISH`.

O status `OK` confirma a gravação local e o encaminhamento pelo socket PUB, sem garantir recebimento por todos os assinantes.

## Relógio lógico

Cada processo mantém um contador inicialmente igual a zero.

Antes de cada envio, incrementa o contador e inclui o valor no campo `logical_clock`. Na recepção, aplica:

```text
contador = max(contador_local, contador_recebido)
```

Não há incremento adicional na recepção.

Essa regra se aplica às mensagens REQ, REP, PUBLICATION e à comunicação com a referência. O contador pertence ao processo e é compartilhado entre seus sockets.

As inscrições internas do ZeroMQ não são envelopes da aplicação.

## Serviço de referência

A referência mantém ranks e informações de disponibilidade dos servidores.

| Operação | Entrada | Resposta |
|---|---|---|
| REGISTER | server_id | Rank e horário da referência |
| LIST_SERVERS | server_id | Nomes e ranks dos servidores ativos |
| HEARTBEAT | server_id | Confirmação, rank e horário da referência |

Na inicialização, cada servidor repete `REGISTER` até obter resposta e consulta `LIST_SERVERS`.

Registros repetidos com o mesmo nome preservam o rank durante a execução da referência.

A cada dez mensagens de clientes recebidas, incluindo retransmissões e mensagens inválidas, o servidor envia `HEARTBEAT` após responder ao cliente.

O timeout da referência é de dois segundos. Uma falha não interrompe permanentemente o atendimento: o próximo bloco de dez mensagens provoca nova tentativa.

## Disponibilidade

Um servidor expira após 60 segundos sem heartbeat. O intervalo pode ser configurado por `REFERENCE_TTL_SECONDS`.

A remoção é verificada mesmo sem novas requisições. Como o heartbeat depende da quantidade de mensagens, servidores sem tráfego também podem expirar.

O próximo heartbeat reativa o servidor com o mesmo rank enquanto a referência permanece em execução.

Após reiniciar a referência, o cadastro é reconstruído pelos próximos heartbeats. Os ranks podem mudar entre reinicializações.

Para verificar a expiração:

```bash
docker compose stop python-server-1
docker compose logs -f reference
```

Após o intervalo sem heartbeat, o log registra `EXPIRED python-server-1`.

Para reiniciar o servidor:

```bash
docker compose start python-server-1
```

## Relógio físico

A referência utiliza seu relógio de sistema. As respostas de `REGISTER` e `HEARTBEAT` incluem esse horário.

O servidor estima a latência de ida como metade do RTT, medido com relógio monotônico, e calcula um offset em milissegundos.

Os timestamps passam a utilizar:

```text
timestamp = hora_do_sistema + offset
```

O relógio do host e do container não é alterado. A estimativa pressupõe atrasos aproximadamente simétricos.

Esta versão não implementa eleição de coordenador nem sincronização Berkeley.

## Retransmissões e deduplicação

Em timeout de três segundos, o bot recria o socket REQ e repete a operação com o mesmo identificador e conteúdo.

Cada envio, inclusive retransmissões, atualiza timestamp e relógio lógico.

A deduplicação ignora esses dois campos de transporte e compara o restante do envelope. Requisições com o mesmo identificador e conteúdo retornam a resposta armazenada no servidor. Conteúdo diferente com o mesmo identificador retorna `ID_CONFLICT`.

Requisição, resposta e efeitos são gravados na mesma transação SQLite.

A deduplicação é local, sem garantia global de processamento exatamente uma vez.

## Persistência e limitações

Cada servidor mantém `/data/chat.db` em volume Docker exclusivo, com as tabelas:

- `logins`.
- `channels`.
- `requests`.
- `publications`.

As consultas utilizam parâmetros SQL.

Não há replicação. Uma listagem pode retornar canais diferentes conforme o servidor responsável, e `ALREADY_EXISTS` considera somente o banco local.

Broker, proxy e referência são componentes únicos. O Pub/Sub não recupera automaticamente mensagens perdidas durante desconexões.

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
python tests/parte3.py
```

Os testes utilizam processos, portas e bancos temporários. Verificam comunicação entre linguagens, persistência, publicações, cadastro, ranks, expiração, reativação, relógio lógico, sincronização física e heartbeat.

# ChatFEI — Parte 4

Sistema de chat distribuído em Python e Java com Request-Reply, Publish-Subscribe, persistência SQLite, relógios lógicos, serviço de referência, eleição de coordenador e sincronização Berkeley.

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — cliente e servidor Python.
- Yuri Lucas Oishi | 22.225.025-0 — cliente e servidor Java.

## Tecnologias

- Python e PyZMQ.
- Java e JeroMQ.
- Protocol Buffers.
- SQLite.
- Docker e Docker Compose.

## Execução

Com o Docker Desktop iniciado, execute na pasta que contém `docker-compose.yaml`:

```bash
docker compose up --build
```

O Compose inicia onze containers:

- Dois bots Python.
- Dois bots Java.
- Dois servidores Python.
- Dois servidores Java.
- Um broker Request-Reply.
- Um proxy Publish-Subscribe.
- Um serviço de referência.

A primeira execução baixa imagens e dependências. Não é necessário instalar localmente Python, Java, Maven ou Protobuf para executar pelo Docker.

Os bots fazem login, consultam e criam canais, inscrevem-se em até três canais e publicam mensagens automaticamente.

Para acompanhar os servidores:

```bash
docker compose logs -f python-server-1 python-server-2 java-server-1 java-server-2
```

Para acompanhar os bots:

```bash
docker compose logs -f python-bot-1 java-bot-1
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

Os bots REQ se conectam ao ROUTER do broker na porta 5555. O DEALER, na porta 5556, distribui as requisições aos servidores REP.

Os servidores gravam publicações no SQLite e as encaminham pelo socket PUB ao XSUB do proxy, na porta 5557. Os bots SUB recebem as mensagens pelo XPUB, na porta 5558.

Cada publicação contém o canal no primeiro frame e um envelope Protobuf no segundo. Os bots verificam a igualdade do canal, pois o filtro do ZeroMQ utiliza prefixos.

A referência atende na porta 5559. O controle entre servidores utiliza REQ/REP na porta 5560.

As portas são internas à rede Compose. Cada servidor possui seu próprio volume SQLite.

Python utiliza PyZMQ e Java utiliza JeroMQ. O contrato comum está em `proto/chat.proto`. Protobuf serializa as mensagens e ZeroMQ realiza o transporte, sem gRPC.

## Operações dos usuários

| Operação | Entrada adicional | Resultado |
|---|---|---|
| LOGIN | Nenhuma | Registra a entrada do usuário |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | Nenhuma | Lista dos canais locais |
| PUBLISH | channel e body | Registra e publica a mensagem |

As requisições incluem UUID, timestamp, relógio lógico, operação e usuário.

Nomes de usuários e canais diferenciam maiúsculas de minúsculas e aceitam de 1 a 32 caracteres ASCII: letras, números, `_` e `-`.

O login não utiliza senha nem sessão de autenticação. Publicações devem conter texto e ter até 4096 bytes UTF-8.

Ao receber `PUBLISH` para um canal válido ainda desconhecido, o servidor registra esse canal localmente.

O status `OK` confirma o registro local e o encaminhamento pelo socket PUB, sem garantir recebimento por todos os assinantes.

## Relógio lógico e concorrência

Cada processo incrementa seu contador antes dos envios e aplica `max(local, recebido)` nas recepções, sem incremento adicional na recepção.

O controle de coordenação executa em threads separadas do atendimento aos bots. Cada thread utiliza seus próprios sockets. O contador lógico é protegido por exclusão mútua.

## Serviço de referência

A referência atribui ranks, lista servidores ativos e recebe heartbeat após cada dez requisições recebidas pelos servidores.

Servidores sem heartbeat por 60 segundos são removidos da lista de ativos. Como o heartbeat depende de tráfego, um servidor ocioso também pode expirar.

Os ranks permanecem estáveis durante a execução da referência.

As respostas de `REGISTER` e `HEARTBEAT` mantêm o timestamp do envelope, mas esse valor não é utilizado para ajustar os relógios nesta versão.

## Eleição de coordenador

Cada servidor consulta os servidores cadastrados e envia mensagens `ELECTION` à porta de controle 5560 dos pares, com timeout de 700 milissegundos.

O servidor de maior rank entre os respondentes é escolhido como coordenador.

A eleição utiliza comparação de ranks, inspirada no algoritmo do valentão, com sondagem periódica a cada dois segundos para detectar falhas, novos servidores e mudanças de disponibilidade.

O coordenador anuncia seu nome no tópico reservado `servers`, utilizando a operação `COORDINATOR`. Todos os servidores se inscrevem nesse tópico.

O anúncio é repetido para reduzir o efeito de perdas do Pub/Sub durante conexões iniciais.

Quando o coordenador falha, os servidores sobreviventes repetem a eleição sem depender dos 60 segundos de expiração na referência.

O mecanismo pressupõe uma rede conectada. Não utiliza consenso distribuído e não garante coordenador único durante partições de rede.

## Sincronização Berkeley

A cada quinze mensagens de aplicação trocadas pelo servidor, uma sincronização é solicitada à thread de controle.

Entram nessa contagem:

- Requisições REQ recebidas.
- Respostas REP enviadas.
- Publicações PUB enviadas.

Mensagens de controle não entram na contagem. Solicitações simultâneas podem ser agrupadas enquanto uma rodada está em andamento.

O coordenador:

1. Coleta os relógios dos servidores com `TIME`.
2. Estima a latência de ida como metade do RTT, usando relógio monotônico.
3. Calcula a média das diferenças, incluindo seu próprio relógio com diferença zero.
4. Envia correções individuais com `ADJUST`.
5. Atualiza seu próprio offset.

Os seguidores podem consultar a hora do coordenador com `CLOCK`.

Uma mensagem `ADJUST` somente é aceita quando identifica o coordenador atualmente reconhecido.

Os ajustes utilizam offsets da aplicação, sem modificar o relógio do host e sem exigir privilégios especiais.

## Verificação da coordenação

Nos logs dos servidores, os registros `ELECTED`, `COORDINATOR`, `TIME`, `ADJUST`, `CLOCK` e `BERKELEY` identificam as operações de controle.

O rank depende da ordem de registro. Qualquer um dos quatro servidores pode ser coordenador.

Após identificar o serviço coordenador nos logs, é possível interrompê-lo:

```bash
docker compose stop NOME_DO_SERVICO
```

Os servidores restantes executam nova eleição.

Para reiniciar o serviço:

```bash
docker compose start NOME_DO_SERVICO
```

Em testes locais, `CONTROL_BIND` e `CONTROL_ENDPOINTS` permitem configurar portas individuais para os processos.

## Retransmissões e deduplicação

Em timeout de três segundos, o bot recria o socket REQ e retransmite a operação com o mesmo identificador e conteúdo.

Timestamp e relógio lógico são atualizados a cada envio. A deduplicação desconsidera esses dois campos e compara o restante do envelope.

O servidor grava requisição, resposta e efeitos na mesma transação. Repetições com o mesmo conteúdo retornam a resposta armazenada. Outro conteúdo com o mesmo identificador retorna `ID_CONFLICT`.

A deduplicação é local, sem garantia global de processamento exatamente uma vez.

## Persistência e limitações

Cada servidor possui um arquivo `/data/chat.db` e um volume Docker exclusivo.

As tabelas são:

- `logins`.
- `channels`.
- `requests`.
- `publications`.

As consultas utilizam parâmetros SQL. Os dados permanecem após reiniciar o container quando o volume é preservado.

Não há replicação nesta versão. Os catálogos e históricos permanecem locais, e a listagem de canais depende do servidor que atende a requisição.

Broker, proxy e referência são componentes únicos. O Pub/Sub não recupera automaticamente mensagens perdidas.

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
python tests/parte4.py
```

A suíte verifica eleição pelo maior rank, comunicação de controle entre Python e Java, anúncio Pub/Sub, sincronização Berkeley e troca de coordenador após falha.

Os testes utilizam portas e bancos temporários e encerram os processos ao finalizar.

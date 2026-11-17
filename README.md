# ChatFEI

Sistema de chat distribuído com clientes e servidores em Python e Java. Utiliza ZeroMQ e Protocol Buffers para comunicação, SQLite para persistência local e replicação periódica entre servidores.

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — cliente e servidor Python.
- Yuri Lucas Oishi | 22.225.025-0 — cliente e servidor Java.

## Funcionalidades

- Login sem senha, criação e listagem de canais.
- Publicação e recebimento de mensagens por canais.
- Relógio lógico nas mensagens.
- Registro de servidores, ranks e heartbeat.
- Eleição de coordenador.
- Sincronização de relógios pelo algoritmo Berkeley.
- Replicação de logins, canais, requisições, respostas e publicações.
- Recuperação de registros após indisponibilidade.
- Consulta paginada do histórico.

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

A primeira execução baixa imagens e dependências. Os bots operam automaticamente.

Para acompanhar os servidores:

```bash
docker compose logs -f python-server-1 java-server-1
```

Para acompanhar os bots:

```bash
docker compose logs -f python-bot-1 java-bot-1
```

Para encerrar:

```bash
docker compose down
```

Os quatro volumes SQLite são independentes e permanecem após o encerramento.

Para apagar também os dados persistidos:

```bash
docker compose down -v
```

## Arquitetura

### Request-Reply

Os bots REQ se conectam ao ROUTER do broker na porta 5555. O DEALER, na porta 5556, distribui as requisições aos servidores REP.

O broker preserva os frames de roteamento e encaminha os envelopes sem interpretar seu conteúdo.

### Publish-Subscribe

Os servidores PUB se conectam ao XSUB do proxy na porta 5557. Os assinantes utilizam o XPUB na porta 5558.

O primeiro frame contém o tópico e o segundo contém o envelope Protobuf binário.

Os bots verificam a igualdade do tópico, pois o filtro nativo do ZeroMQ utiliza correspondência por prefixo.

### Comunicação entre linguagens

Python utiliza PyZMQ e Java utiliza JeroMQ. O contrato único está em `proto/chat.proto`.

O build gera as classes Protobuf para cada linguagem. O Protobuf realiza a serialização e o ZeroMQ realiza o transporte, sem gRPC.

Os clientes são bots automáticos, sem interface gráfica.

### Portas internas

| Porta | Finalidade |
|---|---|
| 5555 | Entrada de requisições no broker |
| 5556 | Encaminhamento do broker aos servidores |
| 5557 | Entrada de publicações no proxy |
| 5558 | Saída de publicações para assinantes |
| 5559 | Serviço de referência |
| 5560 | Controle entre servidores |
| 5561 | Consulta de registros para replicação |

## Serviço de referência

A referência atribui ranks e lista servidores ativos.

O heartbeat é enviado após cada dez requisições recebidas pelo servidor. Um servidor expira após 60 segundos sem heartbeat, inclusive quando está sem tráfego.

Ao retornar, o servidor se registra novamente. Os ranks permanecem estáveis durante a execução da referência.

## Eleição de coordenador

Cada servidor sonda os pares com `ELECTION` a cada dois segundos e escolhe o maior rank disponível.

Uma resposta isolada ausente não remove o par da eleição. São necessárias duas falhas consecutivas, com timeout de 1,5 segundo por sonda. Isso reduz trocas de coordenador provocadas por atrasos momentâneos.

O coordenador anuncia seu nome com `COORDINATOR` no tópico reservado `servers`.

O mecanismo pressupõe uma rede conectada e pode produzir coordenadores distintos durante partições de rede. Não utiliza consenso distribuído.

## Relógios e sincronização

### Relógio lógico

O contador é incrementado antes de cada envio. Na recepção, aplica-se:

```text
contador = max(contador_local, contador_recebido)
```

Não há incremento adicional na recepção.

Cada thread utiliza seus próprios sockets e conexão SQLite. O contador lógico é protegido por exclusão mútua.

### Berkeley

A cada quinze mensagens de aplicação trocadas pelo servidor, é solicitada uma sincronização.

A contagem inclui REQ recebida, REP enviada e PUB enviada. A thread de controle pode agrupar solicitações enquanto uma rodada está em andamento.

O coordenador coleta os relógios com `TIME`, estima metade do RTT, calcula a média das diferenças incluindo seu próprio relógio e envia correções individuais com `ADJUST`.

Os seguidores consultam `CLOCK` para obter a hora do coordenador.

Os ajustes utilizam offsets da aplicação, sem modificar o relógio do host. O heartbeat confirma a disponibilidade do servidor e não fornece um valor de horário para sincronização. O envelope mantém o timestamp de envio obrigatório do protocolo. A sincronização física utiliza o coordenador eleito.

## Organização do código

| Arquivo ou diretório | Responsabilidade |
|---|---|
| python/app.py | Referência, SQLite, bots, servidores, broker e proxy |
| python/coordination.py | Eleição e sincronização na implementação Python |
| python/replication.py | Exportação, comparação e importação de registros em Python |
| java/src/main/java/br/fei/chat/Main.java | Contrato, bot e servidor Java |
| java/src/main/java/br/fei/chat/Coordination.java | Eleição e sincronização na implementação Java |
| java/src/main/java/br/fei/chat/Replication.java | Replicação na implementação Java |
| proto/chat.proto | Definição das mensagens compartilhadas |
| tests/ | Testes de integração |

## Replicação

O sistema utiliza replicação de registros por pull, com múltiplos servidores aceitando escritas e consistência eventual.

Cada servidor atende requisições e armazena seus resultados localmente. Uma thread consulta periodicamente os pares e copia registros persistidos, sem reexecutar operações de usuário nem republicar mensagens.

Os pares são descobertos pela referência. A porta 5561 atende consultas `REPLICA`.

São replicadas as tabelas:

- `logins`.
- `channels`.
- `requests`.
- `publications`.

A transferência ocorre em páginas de até 128 registros, ordenadas pela chave primária.

Cada rodada recomeça do início, permitindo recuperar registros inseridos antes do cursor de uma rodada anterior e alterações resultantes de conflitos. O intervalo entre rodadas é de dois segundos.

Um servidor novo ou com banco vazio consulta os pares disponíveis. Uma falha de transferência é tratada por nova tentativa em outra rodada.

### Justificativa

O broker distribui as escritas entre diferentes servidores. A consulta periódica reúne esses registros sem depender exclusivamente de notificações Pub/Sub, que podem ser perdidas.

A comparação antes da gravação torna a reaplicação idempotente. A recuperação utiliza o mesmo mecanismo de consulta e importação dos registros.

### Adaptações do método

Como o broker distribui requisições entre todos os servidores, não existe um único servidor primário responsável pelas escritas. Cada servidor aceita operações localmente e importa os registros persistidos pelos pares.

A replicação transfere o estado armazenado, sem reexecutar operações nem republicar mensagens. Foram adicionadas paginação, reaplicação idempotente e uma regra determinística para resolver conflitos de mesma chave.

A convergência ocorre após a propagação dos registros, desde que os servidores possam se comunicar. Não há igualdade instantânea entre os bancos. O coordenador eleito é responsável pela sincronização dos relógios, não pela centralização das escritas.

### Formato dos registros

`ReplicaRow` contém o nome da tabela e os valores das colunas na ordem do esquema.

`ReplicaBatch` contém:

- Linhas da página.
- Cursor `next_key`.
- Indicador `more`.

O lote é serializado com Protobuf e codificado em Base64 no campo `body` do envelope. O envelope também é transmitido como Protobuf binário.

Os campos BLOB da tabela `requests` são representados em hexadecimal dentro da linha.

### Resolução de conflitos

Quando duas linhas possuem a mesma chave, as implementações escolhem a menor representação hexadecimal da linha Protobuf completa, em ordem lexicográfica.

A regra é determinística e não prioriza o timestamp mais recente. Dessa forma, a resolução não depende da sincronização dos relógios físicos.

As gravações utilizam transações SQLite. As conexões aguardam até 30 segundos por uma trava de escrita.

Para um conjunto finito de registros, quando as escritas cessam e os pares voltam a se comunicar, a união das chaves e o desempate determinístico permitem a convergência dos bancos.

A sincronização das quatro tabelas pode ser observada parcialmente durante uma rodada.

### Garantias e limitações

O ACK enviado ao bot confirma o commit local. O servidor não espera confirmação das outras réplicas.

Existe uma janela em que uma escrita está armazenada somente no servidor que a recebeu. Se essa única cópia for destruída antes da replicação, o registro pode ser perdido.

A replicação é eventual e não utiliza quórum síncrono.

Reiniciar um servidor com seu volume preservado mantém os registros já gravados localmente. Registros disponíveis nos pares podem ser recuperados após o retorno.

Não há exclusão de registros. Uma operação de exclusão exigiria um mecanismo adicional, como tombstones, para propagar a remoção.

## Contrato dos usuários

Cada requisição contém:

- `request_id`: UUID.
- `timestamp_ms`: timestamp.
- `logical_clock`: contador lógico.
- `operation`: operação.
- `username`: usuário.

Nomes de usuários e canais aceitam de 1 a 32 caracteres ASCII: letras, números, `_` e `-`.

O login registra a entrada sem autenticação por senha.

| Operação | Campos adicionais | Resultado |
|---|---|---|
| LOGIN | Nenhum | Registra login |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | Nenhum | Lista de canais |
| PUBLISH | channel e body | Registra e publica mensagem |
| HISTORY | body com cursor; vazio na primeira página | ReplicaBatch em Base64 |

### Publicação

As publicações devem conter texto e ter até 4096 bytes UTF-8.

`PUBLISH` registra localmente um canal válido ainda desconhecido. O envio pelo Pub/Sub não garante recebimento por todos os assinantes.

### Histórico

`HISTORY` retorna publicações de todos os canais, com até 128 linhas por página.

Para continuar a consulta:

1. Decodifique o Base64 do campo `body`.
2. Interprete o conteúdo como `ReplicaBatch`.
3. Verifique o campo `more`.
4. Se houver outra página, envie `next_key` no `body` de uma nova requisição, utilizando outro `request_id`.

A ordenação é por identificador. O timestamp está disponível em cada linha para ordenação cronológica pela aplicação consumidora.

### Retransmissões

Retransmissões preservam o identificador e o conteúdo da operação, atualizando timestamp e relógio lógico.

A comparação para deduplicação ignora esses dois campos de transporte.

A reutilização do mesmo identificador com outra operação ou conteúdo retorna `ID_CONFLICT`.

A deduplicação entre réplicas depende da propagação dos registros.

## Verificação do funcionamento

### Comunicação

Após iniciar o Compose, os logs dos bots registram login, criação de canais, publicações e recebimento de mensagens entre Python e Java.

Os logs dos servidores incluem timestamps, relógios lógicos, registro e heartbeat.

### Troca de coordenador

Identifique o coordenador nos registros `ELECTED` e interrompa o serviço correspondente:

```bash
docker compose stop NOME_DO_SERVICO
```

Os servidores restantes executam uma nova eleição.

Para reiniciar o serviço:

```bash
docker compose start NOME_DO_SERVICO
```

### Convergência das réplicas

Os registros `REPLICA MERGED` indicam importação de registros.

Para comparar os bancos após interromper a produção de novas mensagens:

```bash
docker compose stop python-bot-1 python-bot-2 java-bot-1 java-bot-2
```

Aguarde a propagação e compare os identificadores e conteúdos das tabelas SQLite, além das contagens. O histórico também pode ser consultado pela operação `HISTORY`.

Com os bots ativos, diferenças transitórias entre réplicas são esperadas.

Para retomar os bots:

```bash
docker compose start python-bot-1 python-bot-2 java-bot-1 java-bot-2
```

### Recuperação após indisponibilidade

Interrompa um servidor enquanto os bots continuam produzindo mensagens. Reinicie o servidor e aguarde a sincronização.

Os registros produzidos durante a indisponibilidade são recuperados dos pares disponíveis.

## Limitações gerais

- Broker, proxy Pub/Sub e referência são componentes únicos.
- A eleição não garante coordenador único durante partições de rede.
- A replicação não garante leitura imediatamente atualizada em todos os servidores.
- O ACK confirma apenas o commit local.
- A sincronização das tabelas não representa um snapshot global atômico.
- Pub/Sub não garante recebimento de todas as publicações.
- Não há autenticação por senha, controle de acesso ou exclusão de registros.

## Desenvolvimento e testes locais

Requisitos: Python 3.12, Java 17 ou superior e Maven.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r python/requirements.txt
python -m grpc_tools.protoc -Iproto --python_out=python proto/chat.proto
mvn -f java/pom.xml package
```

Execute as suítes:

```bash
python tests/parte4.py
python tests/parte5.py
```

A suíte `parte4.py` verifica eleição, sincronização Berkeley e troca de coordenador com a replicação habilitada.

A suíte `parte5.py` verifica igualdade dos registros entre quatro réplicas, paginação do histórico e recuperação de um servidor com banco vazio, incluindo mensagens produzidas durante sua indisponibilidade.

Os arquivos `integration.py` e `parte3.py` correspondem a cenários das versões anteriores.

Os testes utilizam processos Python e Java, sockets ZeroMQ, mensagens Protobuf e bancos SQLite temporários.

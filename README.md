# ChatFEI — Parte 4

Projeto de Sistemas Paralelos e Distribuídos (CC7261/FEI), para uma dupla.
Implementa login, criação e listagem de canais, publicação de mensagens e inscrição em canais. Esta entrega cobre as Partes 1 a 4: relógios, referência, eleição e Berkeley.

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — Python (cliente e servidor).
- Yuri Lucas Oishi | 22.225.025-0 — Java (cliente e servidor).

## Executar

Instale e inicie o Docker Desktop (inclui Compose). Extraia o ZIP, abra a pasta `chatfei` no VSCode e execute no terminal:

```bash
docker compose up --build
```

O comando inicia 11 containers: 2 bots Python, 2 bots Java, 2 servidores Python, 2 servidores Java, 1 broker Request-Reply 1 proxy Pub/Sub e 1 serviço de referência. Não é necessário instalar Python, Java, Maven ou Protobuf no computador. A primeira execução precisa de internet para baixar imagens e dependências.

Os bots fazem login e descobrem canais pelos servidores. Enquanto conhecem menos de cinco canais, tentam criar um canal novo. A cada rodada, cada bot se inscreve em mais um canal aleatório, até três inscrições. Depois escolhe um canal conhecido e envia dez mensagens de conteúdo aleatório, com uma pausa de um segundo entre os envios.

```bash
docker compose logs -f python-bot-1 java-bot-1
```

Nos logs, procure por `SUB inscrito em`, por uma resposta `PUBLISH` com `OK` e por `SUB RECV` com `PUBLICATION`. O campo `timestamp_ms` da publicação representa o horário do envio pelo servidor. O campo `received_at_ms` registra o horário da recepção pelo bot. O campo `server_id` identifica o servidor que publicou.

`down` preserva os volumes. Para reiniciar com dados vazios, **apagando os dados da demonstração**, use `docker compose down -v`.

## Arquitetura e escolhas

Bots REQ conectam ao broker ROUTER (`5555`). O broker encaminha as mensagens pelo DEALER (`5556`) aos servidores REP, distribuindo as requisições entre conexões disponíveis. As portas são internas à rede Compose. O broker preserva os frames de roteamento e não interpreta os dados.

O bot pede a publicação via Request-Reply. O servidor grava a mensagem no SQLite, envia a publicação pelo socket PUB e responde ao bot. Os servidores PUB se conectam ao XSUB do proxy `pubsub:5557`. Os bots SUB se conectam ao XPUB em `pubsub:5558`. O proxy roda em um container separado e encaminha mensagens e inscrições. O tópico é o nome do canal no primeiro frame. O segundo frame contém o envelope Protobuf com operação `PUBLICATION`. Os bots verificam a igualdade do nome, pois o filtro SUB de ZeroMQ funciona por prefixo.

O status `OK` confirma o registro local e o encaminhamento pelo socket PUB. Um assinante recebe as publicações enquanto está conectado e inscrito. Mensagens anteriores à inscrição ou perdidas durante uma desconexão continuam no SQLite do servidor que as gravou.

- Python: PyZMQ. Java: JeroMQ. Todos usam o mesmo contrato ZeroMQ e Protobuf.
- Protobuf: `proto/chat.proto` é a única definição do contrato. O build gera as classes Python e Java. Não usamos gRPC: Protobuf serializa e ZeroMQ transporta.
- SQLite: um arquivo `/data/chat.db` e um volume Docker exclusivo por servidor. Não existe banco compartilhado.
- O broker é um componente auxiliar em Python, reutilizado pelas duas implementações.
- Sem interface gráfica: os usuários são bots automáticos, conforme o enunciado.

## Contrato e regras

Cada requisição contém `request_id` (UUID), `timestamp_ms` (Unix UTC em milissegundos), `operation` e `username`. Criação também contém `channel`. Respostas contêm o mesmo ID, timestamp próprio, operação, `status`, `detail`, `server_id` e, na listagem, `channels`.

| Operação | Entrada adicional | Resultado |
|---|---|---|
| LOGIN | Nenhuma | OK; registra nome e timestamp, sem senha |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | Nenhuma | OK e lista ordenada de nomes |
| PUBLISH | channel e body | OK, INVALID_CHANNEL ou INVALID_BODY |

Nomes são sensíveis a maiúsculas/minúsculas e aceitam 1–32 caracteres ASCII: letras, números, `_` e `-`. Não há sessão de autenticação: o login é um registro de entrada, e cada operação contém o usuário. Isso permite que requisições seguintes sejam atendidas por outro servidor. Senhas e controle de acesso não fazem parte deste protótipo.

O corpo da publicação deve ter conteúdo e pode ter até 4096 bytes em UTF-8. Como os catálogos ainda são locais e o broker pode encaminhar uma publicação a outro servidor, o servidor que recebe `PUBLISH` registra localmente o tópico válido se ainda não o conhece. Essa operação não consulta os outros bancos.

Erros: INVALID_BODY, INVALID_REQUEST, INVALID_USER, INVALID_CHANNEL, UNKNOWN_OPERATION, INVALID_PROTOBUF, STORAGE_ERROR e ID_CONFLICT. Os logs podem ser textuais; o corpo de todas as mensagens na rede é Protobuf binário.

O bot repete o login em caso de erro. Em timeout de 3 segundos, fecha o socket REQ e cria outro, repetindo a mesma operação com o mesmo ID e conteúdo da operação. Cada envio, inclusive retransmissões, atualiza timestamp e relógio lógico. A deduplicação ignora somente estes dois campos de transporte e compara o restante do envelope. Cada resposta recebe timestamp e contador atualizados.

O servidor salva requisição e resposta na mesma transação que seus efeitos. Repetir o mesmo ID e conteúdo no mesmo servidor devolve a resposta sem repetir o efeito. Reutilizar ID com conteúdo diferente retorna ID_CONFLICT. Essa deduplicação é **local**: uma retransmissão atendida por outro servidor pode ser registrada novamente. Não há garantia global de exactly-once.

## Persistência e limitações desta etapa

Tabelas: `logins` (ID, usuário, timestamp), `channels` (nome, criador, timestamp) `requests` (ID e envelopes binários da requisição/resposta) e `publications` (ID, canal, autor, horário do envio e texto). As consultas usam parâmetros SQL. O banco permanece disponível após reiniciar o container.

Até a Parte 5, cada servidor tem somente seus próprios registros. Assim, uma listagem balanceada pode retornar canais diferentes; ALREADY_EXISTS vale para o servidor que recebeu o pedido. Nenhum arquivo é compartilhado para contornar essa característica. A disponibilidade depende do broker, que é um ponto único de falha nesta versão.

Falta a replicação e recuperação do histórico completo (Parte 5).

## Desenvolvimento local

Python 3.12:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r python/requirements.txt
python -m grpc_tools.protoc -Iproto --python_out=python proto/chat.proto
```

Java 17 e Maven:

```bash
mvn -f java/pom.xml package
```

Para executar verificações de integração locais após preparar as duas linguagens:

```bash
python tests/parte4.py
```

O script usa portas locais temporárias, bancos temporários e encerra seus processos ao finalizar. Consulte `docs/VALIDACAO.md` para o resultado obtido durante a preparação.

## Organização da entrega

A Parte 4 deve ser publicada na branch `parte4` a partir de `parte3`. No Moodle, entregue este ZIP e o link da branch `parte4`. O ZIP não cria nem publica uma branch automaticamente.

## Referência

[Enunciado do professor no commit utilizado](https://gitlab.com/laferreira/fei/cc7261/-/tree/e1aa88a80482869a9028643c2172a4d9983e0a2a/projeto).



## Parte 4 — Eleição e Berkeley

Cada servidor mantém o nome do coordenador e executa controle em threads
separadas do atendimento aos bots. Os sockets são exclusivos de cada thread;
o contador lógico usa exclusão mútua. O contador continua incrementando no
envio e usando max(local, recebido) na recepção, conforme a Parte 3.

A referência continua atribuindo ranks e removendo servidores sem heartbeat
após 60 segundos. Ela não é mais fonte de sincronização: REGISTER e HEARTBEAT
mantêm somente o timestamp obrigatório do envelope; os servidores não usam
esse valor para ajustar o relógio. O heartbeat continua a cada dez requisições.

### Eleição
Cada servidor consulta os cadastrados e envia ELECTION por REQ/REP à porta
5560 de cada par, com timeout de 700 ms. O maior rank entre os respondentes
vence. É uma eleição por comparação de ranks, inspirada no algoritmo do
valentão, com sondagem periódica a cada dois segundos para convergência,
detecção de falhas e entrada de servidores novos. O vencedor publica seu nome
no tópico reservado `servers`, com operação COORDINATOR, pelo proxy existente.
Todos os servidores se inscrevem nesse tópico. O anúncio é repetido para
compensar perdas do Pub/Sub durante conexões iniciais.

Uma falha do coordenador faz os sobreviventes repetirem a eleição sem esperar
os 60 segundos de expiração na referência. A implementação pressupõe uma rede
conectada: não garante coordenador único durante partições de rede, nem usa
consenso distribuído. Os ranks são estáveis durante a execução da referência.

### Sincronização
A cada 15 mensagens de aplicação trocadas pelo servidor (recebimentos REQ,
envios REP e publicações PUB), uma sincronização é solicitada à thread de
controle. Mensagens de controle não entram nessa contagem, evitando ciclos
de sincronização gerados pela própria sincronização. Pedidos simultâneos
podem ser agrupados enquanto uma rodada está em andamento.

O coordenador coleta os relógios com TIME, estima metade do RTT usando relógio
monotônico, calcula a média das diferenças incluindo seu próprio relógio
(diferença zero) e envia a correção individual com ADJUST. Ele também corrige
seu próprio offset. Os seguidores podem pedir CLOCK ao coordenador para obter
a hora corrente, como no diagrama do enunciado. ADJUST só é aceito do nome
atualmente reconhecido como coordenador. Ajustes usam offsets da aplicação,
sem alterar o relógio do computador e sem privilégios especiais.

### Executar e demonstrar
```bash
docker compose up --build
docker compose logs -f python-server-1 python-server-2 java-server-1 java-server-2
```
Procure ELECTED, COORDINATOR, TIME, ADJUST, CLOCK e BERKELEY.
O rank depende da ordem de registro, portanto qualquer um dos quatro
servidores pode ser coordenador. Depois de identificar seu nome nos logs,
pare esse serviço com `docker compose stop NOME_DO_SERVICO` e acompanhe a
nova eleição. Reinicie com `docker compose start NOME_DO_SERVICO`.

A porta 5560 é interna e igual nos containers separados. Para os testes
locais, CONTROL_BIND e CONTROL_ENDPOINTS permitem portas individuais.
Broker, proxy, bots e contrato Protobuf mantêm a compatibilidade da Parte 3.
O Compose conserva os onze containers e os quatro volumes independentes.

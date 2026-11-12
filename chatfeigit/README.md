# ChatFEI — Parte 5 (versão final)

Projeto de Sistemas Paralelos e Distribuídos, CC7261/FEI.

- Lucas de Almeida Brandão, 22.124.066-6 — cliente e servidor Python.
- Yuri Lucas Oishi, 22.225.025-0 — cliente e servidor Java.

## Executar

Com Docker Desktop iniciado, abra um terminal na pasta que contém este README:

```bash
docker compose up --build
```

O Compose inicia onze containers: quatro bots, quatro servidores, broker,
proxy Pub/Sub e referência. Há dois bots e dois servidores por linguagem.
A primeira execução baixa imagens e dependências. Os bots operam automaticamente.

```bash
docker compose logs -f python-server-1 java-server-1
docker compose down
```

Os quatro volumes SQLite são independentes e persistem após `down`.
`docker compose down -v` apaga os dados da demonstração.

## Funcionalidades acumuladas

1. Login sem senha, criação e listagem de canais por Request-Reply.
2. Publicação de mensagens e inscrições aleatórias por Publish-Subscribe.
3. Relógio lógico, cadastro com ranks e heartbeat por dez requisições recebidas.
4. Eleição pelo maior rank respondente, anúncio em `servers` e sincronização Berkeley.
5. Replicação de logins, canais, requisições/respostas e publicações, recuperação de servidores e consulta do histórico.

## Arquitetura

Bots REQ conectam ao ROUTER do broker em 5555. O DEALER em 5556 distribui
requisições aos servidores REP. Servidores PUB usam o XSUB do proxy em 5557,
e assinantes usam XPUB em 5558. O tópico é o primeiro frame e o envelope
Protobuf binário é o segundo. Os bots verificam igualdade do tópico, pois o
filtro nativo do ZeroMQ aceita prefixos.

Python usa PyZMQ e Java usa JeroMQ. O contrato único é `proto/chat.proto`.
O build gera os arquivos Protobuf para cada linguagem. A comunicação usa
ZeroMQ e serialização Protobuf. Não há interface gráfica nem entrada manual
para o funcionamento dos bots.

A referência atende em 5559, atribui ranks e lista servidores ativos. O
heartbeat é enviado após dez requisições. Um servidor expira após 60 segundos
sem heartbeat, inclusive quando está sem tráfego. Ao retornar ele se registra
novamente. Os ranks são estáveis durante a execução da referência.

O controle entre servidores usa REQ/REP em 5560. Cada servidor sonda os pares
a cada dois segundos com ELECTION e escolhe o maior rank disponível. Uma falha
isolada de resposta não remove o par da eleição: são necessárias duas falhas
consecutivas, com timeout de 1,5 segundo por sonda. Isso reduz trocas de líder
causadas por atrasos momentâneos sob carga. O eleito anuncia COORDINATOR pelo
tópico `servers`. O mecanismo pressupõe uma rede conectada e ainda pode ter
líderes distintos durante partições de rede.

A cada quinze mensagens de aplicação trocadas pelo servidor (REQ recebida,
REP enviada ou PUB enviada), solicita-se sincronização. A thread de controle
pode agrupar solicitações enquanto uma rodada está em andamento. O coordenador
coleta TIME, estima metade do RTT, calcula a média incluindo o próprio relógio
e envia ADJUST com correções individuais. Os seguidores consultam CLOCK para
obter a hora do coordenador. Os ajustes usam offsets, sem mudar a hora do host.
A referência conserva o timestamp obrigatório do envelope, mas esse timestamp
não é usado para sincronização após a Parte 4.

O relógio lógico é incrementado antes de cada envio e atualizado por
`max(local, recebido)` na recepção, seguindo a regra específica do enunciado.
Cada thread tem seus próprios sockets e conexão SQLite. O contador lógico
é protegido por exclusão mútua.

## Mapa do código

Os comentários nos fontes explicam as decisões junto ao trecho que as executa.
Para uma leitura guiada, estes são os pontos de entrada principais:

- `python/app.py`: referência, acesso SQLite, operações dos bots/servidores,
  broker e proxy Pub/Sub.
- `python/coordination.py` e `java/src/main/java/br/fei/chat/Coordination.java`: eleição, anúncios
  do coordenador e ajuste Berkeley.
- `python/replication.py` e `java/src/main/java/br/fei/chat/Replication.java`: exportação paginada,
  comparação de conflitos e cópia entre bancos.
- `java/src/main/java/br/fei/chat/Main.java`: implementação Java do contrato, bot e servidor.
- `proto/chat.proto`: campos comuns que garantem a interoperabilidade binária.
- `tests/`: cenários de integração por parte e verificação das duas linguagens.

## Replicação escolhida e justificativa

Usamos **replicação passiva com atualização por pull e consistência eventual**.
Cada servidor atende requisições e armazena seus resultados localmente. Uma
thread consulta periodicamente os outros servidores e copia registros já
persistidos, sem reexecutar operações de usuário nem republicar mensagens.

A porta interna 5561 atende consultas REPLICA. Cada réplica descobre pares
pela referência e percorre as tabelas `logins`, `channels`, `requests` e
`publications`. A transferência é paginada em lotes de até 128 registros,
ordenados pela chave primária. Cada rodada recomeça do início, o que também
recupera registros inseridos em posições anteriores ao cursor e alterações
resultantes de conflitos. A pausa entre rodadas é de dois segundos.

A solução foi escolhida porque o broker distribui as escritas entre servidores.
O pull periódico reúne os fragmentos de histórico sem depender de uma
publicação Pub/Sub que possa ser perdida. Um servidor novo ou com banco vazio
obtém as páginas dos pares disponíveis. Uma falha durante a transferência
provoca nova tentativa em outra rodada. Registros existentes são comparados
antes da gravação, tornando a reaplicação idempotente.

### Contrato e resolução de conflitos

`ReplicaRow` contém nome da tabela e valores das colunas na ordem do esquema.
`ReplicaBatch` contém as linhas, o cursor `next_key` e o indicador `more`.
O lote é serializado com Protobuf e colocado em Base64 no campo `body` do
envelope, que também é transmitido como Protobuf binário. Os campos BLOB da
tabela requests são representados por hexadecimal dentro da linha.

Para a mesma chave, as duas linguagens escolhem a menor representação
hexadecimal da linha Protobuf completa, em ordem lexicográfica. Esta é uma
regra determinística de desempate, sem preferência por horário mais recente.
Ela evita depender dos relógios físicos durante a resolução. As escritas
aplicam a mesma regra em todas as réplicas e ocorrem em transações SQLite.
As conexões aguardam até 30 segundos por uma trava de escrita.

Essa regra é uma adaptação do mecanismo de resolução de conflitos visto em
consistência eventual forte. Para um conjunto finito de registros, quando as
escritas cessam e todos os pares voltam a se comunicar, a união das chaves e
o desempate determinístico fazem os bancos convergirem. A atualização das
quatro tabelas pode ser observada parcialmente durante a sincronização.

Não há exclusão de registros nesta versão. Uma futura operação de exclusão
precisaria de tombstones. O ACK ao bot confirma o commit local. Se a única
cópia de uma escrita for destruída antes de ser copiada, essa escrita pode ser
perdida. Como a replicação roda em ciclos de dois segundos, existe
uma janela em que uma escrita ainda está apenas no servidor que a recebeu; o
sistema não espera confirmação das outras réplicas antes de responder ao bot.
Na apresentação, descreva isso como replicação eventual, sem garantia de
quórum síncrono. Reiniciar com o volume preservado permite recuperar os dados
que já foram gravados localmente.
O broker, o proxy e a referência são componentes únicos nesta arquitetura.

## Contrato dos usuários

Cada requisição contém UUID `request_id`, `timestamp_ms`, `logical_clock`,
`operation` e `username`. Nomes de usuário/canal aceitam 1 a 32 letras ASCII,
números, `_` e `-`. LOGIN registra a entrada sem autenticação por senha.

| Operação | Campos adicionais | Resultado |
|---|---|---|
| LOGIN | — | Registra login |
| CREATE_CHANNEL | channel | OK ou ALREADY_EXISTS |
| LIST_CHANNELS | — | Lista de canais |
| PUBLISH | channel e body | Registra e publica mensagem |
| HISTORY | body com cursor, vazio na primeira página | ReplicaBatch em Base64 no body |

HISTORY retorna publicações de todos os canais, com até 128 linhas por página.
Decodifique Base64 e interprete ReplicaBatch. Se `more` for verdadeiro, use
`next_key` no body da próxima requisição, com um novo request_id. A ordenação
é por ID para permitir paginação estável. O timestamp está disponível em cada
linha para apresentação cronológica.

As publicações aceitam até 4096 bytes UTF-8. Para preservar o comportamento
das etapas anteriores, PUBLISH registra localmente um tópico válido ainda
não conhecido. Retransmissões usam o mesmo ID e conteúdo, com contador e
horário de envio atualizados. A comparação para deduplicação ignora esses dois
campos de transporte. Reutilizar o ID com outra operação/conteúdo retorna
ID_CONFLICT. A deduplicação entre réplicas depende da propagação dos registros.

## Demonstração final

1. Execute o Compose e acompanhe os bots fazendo login, criando canais,
   publicando e recebendo mensagens entre Python e Java.
2. Mostre timestamp e logical_clock nos logs, além de REGISTER e HEARTBEAT.
3. Identifique ELECTED e BERKELEY. Pare o coordenador com
   `docker compose stop NOME_DO_SERVICO` e observe a nova eleição.
4. Observe REPLICA MERGED nos servidores. Para comparar os bancos em um
   instante estável, pause os quatro bots e aguarde a propagação:

```bash
docker compose stop python-bot-1 python-bot-2 java-bot-1 java-bot-2
```

5. Consulte o histórico pela operação HISTORY ou inspecione as tabelas SQLite.
   Compare IDs e conteúdo, além das contagens. Com bots ativos, diferenças
   transitórias são esperadas pelo modelo eventual.
6. Pare um servidor, mantenha os bots produzindo mensagens e reinicie o
   servidor. Aguarde a sincronização e mostre que ele recebeu os registros
   produzidos enquanto estava fora.

Para retomar os bots:

```bash
docker compose start python-bot-1 python-bot-2 java-bot-1 java-bot-2
```

## Testes locais

Requer Python 3.12, Java 17 ou superior e Maven:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r python/requirements.txt
python -m grpc_tools.protoc -Iproto --python_out=python proto/chat.proto
mvn -f java/pom.xml package
python tests/parte4.py
python tests/parte5.py
```

As suítes da versão final são parte4.py e parte5.py. Os arquivos integration.py
e parte3.py documentam testes históricos. Os resultados e os limites da
validação estão em `docs/VALIDACAO.md`.

## Entrega

Publique os arquivos na branch `parte5`, criada a partir de `parte4`, e envie
ao Moodle o ZIP e o link da branch. O ZIP não publica a branch automaticamente.

Enunciado utilizado:
https://gitlab.com/laferreira/fei/cc7261/-/tree/e1aa88a80482869a9028643c2172a4d9983e0a2a/projeto

# ChatFEI — Parte 1

Projeto de Sistemas Paralelos e Distribuídos (CC7261/FEI), para uma dupla.
Implementa login, criação e listagem de canais. **Esta entrega cobre a Parte 1; as Partes 2 a 5 ainda não estão implementadas.**

## Integrantes

- Lucas de Almeida Brandão | 22.124.066-6 — Python (cliente e servidor).
- Yuri Lucas Oishi | 22.225.025-0 — Java (cliente e servidor).

## Executar

Instale e inicie o Docker Desktop (inclui Compose). Extraia o ZIP, abra a pasta `chatfei` no VSCode e execute no terminal:

```bash
docker compose up --build
```

O comando inicia 9 containers: 2 bots Python, 2 bots Java, 2 servidores Python, 2 servidores Java e 1 broker Python. Não é necessário instalar Python, Java, Maven ou Protobuf no computador. A primeira execução precisa de internet para baixar imagens e dependências. Funciona com imagens para Intel/AMD e ARM, incluindo Macs Apple Silicon; a execução Docker ainda precisa ser validada no computador da dupla.

Os bots fazem login, criam `geral` e um canal próprio e listam canais a cada 3 segundos. Os logs mostram SEND/RECV, request_id, timestamp, operação, conteúdo e servidor responsável. Os servidores registram também erros. Ctrl+C interrompe os serviços.

```bash
docker compose logs -f python-server-1 java-server-1
docker compose down
docker compose up
```

`down` preserva os volumes. Para reiniciar com dados vazios, **apagando os dados da demonstração**, use `docker compose down -v`.

## Arquitetura e escolhas

Bots REQ conectam ao broker ROUTER (`5555`). O broker encaminha as mensagens pelo DEALER (`5556`) aos servidores REP, distribuindo as requisições entre conexões disponíveis. As portas são internas à rede Compose. O broker preserva os frames de roteamento e não interpreta os dados.

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

Nomes são sensíveis a maiúsculas/minúsculas e aceitam 1–32 caracteres ASCII: letras, números, `_` e `-`. Não há sessão de autenticação: o login é um registro de entrada, e cada operação contém o usuário. Isso permite que requisições seguintes sejam atendidas por outro servidor. Senhas e controle de acesso não fazem parte deste protótipo.

Erros: INVALID_REQUEST, INVALID_USER, INVALID_CHANNEL, UNKNOWN_OPERATION, INVALID_PROTOBUF, STORAGE_ERROR e ID_CONFLICT. Os logs podem ser textuais; o corpo de todas as mensagens na rede é Protobuf binário.

O bot repete o login em caso de erro. Em timeout de 3 segundos, fecha o socket REQ e cria outro, repetindo a mesma operação com o mesmo ID e timestamp. O timestamp representa a criação/envio inicial da requisição; retransmissões preservam o envelope para identificação de duplicatas. Cada resposta recebe timestamp de envio atualizado.

O servidor salva requisição e resposta na mesma transação que seus efeitos. Repetir o mesmo ID e conteúdo no mesmo servidor devolve a resposta sem repetir o efeito. Reutilizar ID com conteúdo diferente retorna ID_CONFLICT. Essa deduplicação é **local**: uma retransmissão atendida por outro servidor pode ser registrada novamente. Não há garantia global de exactly-once.

## Persistência e limitações desta etapa

Tabelas: `logins` (ID, usuário, timestamp), `channels` (nome, criador, timestamp) e `requests` (ID e envelopes binários da requisição/resposta). As consultas usam parâmetros SQL. O banco permanece disponível após reiniciar o container.

Até a Parte 5, cada servidor tem somente seus próprios registros. Assim, uma listagem balanceada pode retornar canais diferentes; ALREADY_EXISTS vale para o servidor que recebeu o pedido. Nenhum arquivo é compartilhado para contornar essa característica. A disponibilidade depende do broker, que é um ponto único de falha nesta versão.

Ainda faltam: publicações e proxy Pub/Sub (Parte 2), relógios lógicos e referência/heartbeat (Parte 3), eleição e Berkeley (Parte 4), replicação e recuperação do histórico completo (Parte 5).

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
python tests/integration.py
```

O script usa portas locais temporárias, bancos temporários e encerra seus processos ao finalizar. Consulte `docs/VALIDACAO.md` para o resultado obtido durante a preparação.

## Organização da entrega

Crie o repositório do grupo e uma branch para esta etapa:

```bash
git init
git switch -c parte1
git add .
git commit -m "Implementa Parte 1 com Python Java ZeroMQ e Protobuf"
```

Depois configure o remoto do grupo e publique `parte1`. O ZIP não inclui histórico Git e não substitui o link da branch exigido no Moodle. Preencha os integrantes e valide `docker compose up --build` antes de enviar.

## Referência

[Enunciado do professor no commit utilizado](https://gitlab.com/laferreira/fei/cc7261/-/tree/e1aa88a80482869a9028643c2172a4d9983e0a2a/projeto).

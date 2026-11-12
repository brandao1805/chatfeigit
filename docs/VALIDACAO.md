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
instalado. Docker não está disponível neste ambiente. O build Maven dentro
da imagem e a execução Docker Compose devem ser confirmados no computador
da dupla com `docker compose up --build`.

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

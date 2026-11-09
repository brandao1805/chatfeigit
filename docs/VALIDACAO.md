# Validação da Parte 2

- Java compilado e empacotado com Maven. Contrato Python gerado com Protobuf.
- Integração ZeroMQ via TCP em processos locais. Um bot Java recebeu publicações de um servidor Python e um bot Python recebeu publicações de um servidor Java, em ambos os casos pelo proxy XSUB/XPUB.
- Foram verificadas as operações da Parte 1, a rejeição de publicação vazia, a deduplicação local de uma publicação repetida e a recuperação de autor, canal, horário e texto no SQLite após reiniciar cada servidor.
- A topologia com 10 processos rodou com broker, proxy Pub/Sub, quatro bots e quatro servidores. Os bots receberam publicações e os quatro bancos separados registraram requisições.
- O YAML do Compose foi conferido com 10 serviços e 4 volumes. O Docker não estava disponível neste ambiente. A execução pelo Compose deve ser conferida no Docker Desktop.

Para reproduzir a integração local, gere as classes Protobuf para Python, empacote o Java e execute `python tests/integration.py`, conforme o README.

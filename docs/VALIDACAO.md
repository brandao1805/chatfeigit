# Validação da Parte 1

Data: 05/11/2026.

## Verificações executadas

- Compilação e empacotamento Java com Maven: BUILD SUCCESS.
- Geração do contrato Python a partir de `proto/chat.proto`.
- Integração TCP/ZeroMQ real, com mensagens Protobuf:
  - bot Java -> servidor Python;
  - bot Python -> servidor Java;
  - login, criação e listagem;
  - canal duplicado, nomes inválidos, operação desconhecida e Protobuf inválido;
  - repetição do mesmo ID sem duplicar login no mesmo servidor;
  - rejeição de ID reutilizado com conteúdo diferente;
  - leitura dos canais após encerrar e reiniciar o servidor;
  - execução da topologia com 9 processos: broker, quatro bots e quatro servidores;
  - confirmação de requisições persistidas nos quatro bancos independentes.
- YAML do Compose analisado: 9 serviços e 4 volumes exclusivos.

## Limite da validação

Docker não está disponível no ambiente de desenvolvimento utilizado. Portanto, os testes de integração rodaram como processos locais; não foram executados o build das imagens nem `docker compose up`. A dupla deve realizar essa verificação no Docker Desktop antes de entregar.

Os testes são reproduzíveis por `python tests/integration.py`, depois de instalar as dependências, gerar o contrato Python e empacotar o Java conforme o README. Os bancos usados nos testes são temporários e são removidos ao final.

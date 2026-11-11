# Validação da Parte 4

Teste desta etapa: `python tests/parte4.py`, após gerar Protobuf e compilar Java.
Verifica eleição pelo maior rank, comunicação de controle Python/Java,
anúncio Pub/Sub, coleta de relógios, ajuste Berkeley e troca do coordenador
após encerrar o servidor Java.

Java compilado com ECJ no nível Java 17, usando o contrato e dependências
existentes da Parte 3. Docker não está disponível neste ambiente; é necessário
confirmar `docker compose up --build` no computador da dupla.

Os testes integration.py e parte3.py são históricos das etapas anteriores;
a suíte indicada para esta entrega é parte4.py.

## Resultado

```text
PASS eleição: Java (maior rank) coordenador, Python seguidor
PASS Berkeley: coleta TIME, média, ADJUST e anúncio Pub/Sub entre linguagens
PASS falha: Python assume após desligamento do coordenador Java
```

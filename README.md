## Projeto realizado no âmbito da disciplina de Algoritmos Distribuidos FCT NOVA
Bernardo Ascensão 75153
Guilherme Ribeiro 75539
Rafael Fandiño 75246

-------------------

## Para executar, utilize os seguintes comandos:

O comando abaixo deve ser feito em terminais diferentes, a quantidade de vezes depende do número de réplicas desejadas.

**java -jar target/DistAlg.jar babel.address=IPx agreement_proto=(raft/multi-paxos) initial_membership=IP1:PORT1,IP2:PORT2,IPX:PORTX**

---

exemplo:

*java -jar target/DistAlg.jar babel.address=127.0.0.1 agreement_proto=raft initial_membership=127.0.0.1:34000,127.0.0.1:34001,127.0.0.1:34002*

## TESTES:

- Teste singular:
**bash exec.sh 4 1000 "127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002" 0.5 0.5**

- Conjunto de todos os testes necessários para gerar uma linha completa no gráfico:
**bash genall.sh multi-paxos/raft**

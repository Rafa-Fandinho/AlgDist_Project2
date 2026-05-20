#!/bin/bash

# Lista de threads pedida (talvez mudar se for necessário!)
THREADS=(1 2 4 6 8 10 12 16 20 24 28 32 40 48 56 64 80 96 128)

# Cabeçalho do ficheiro de resultados
echo "threads,throughput,latency_ms" > resultados.csv

echo "Iniciando Benchmark..."

for t in "${THREADS[@]}"
do
    echo "--------------------------------------"
    echo "A testar com $t threads..."
    
    # Executa o cliente (Garante que o caminho para o exec.sh está correto)
    # Se o benchmark.sh estiver na mesma pasta que o exec.sh, usa ./exec.sh
    bash exec.sh $t 1000 127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002 50 50 > temp_out.txt 2>&1
    
    # Extrai o Throughput
    thr=$(grep "\[OVERALL\], Throughput(ops/sec)" temp_out.txt | cut -d',' -f3 | tr -d ' ' | tr -d '\r')
    
    # Extrai as latências de READ e UPDATE (em microssegundos)
    read_lat=$(grep "\[READ\], AverageLatency(us)" temp_out.txt | cut -d',' -f3 | tr -d ' ' | tr -d '\r')
    update_lat=$(grep "\[UPDATE\], AverageLatency(us)" temp_out.txt | cut -d',' -f3 | tr -d ' ' | tr -d '\r')
    
    # Se os valores forem vazios (erro), mete 0
    read_lat=${read_lat:-0}
    update_lat=${update_lat:-0}
    thr=${thr:-0}
    
    # Cálculo da latência média ponderada
    # avg_lat=$(echo "scale=4; (($read_lat + $update_lat) / 2) / 1000" | bc -l)
    avg_lat=$(python -c "print((($read_lat + $update_lat) / 2.0) / 1000.0)")

    echo "Thread $t: $thr ops/sec | $avg_lat ms"
    
    # Guarda no CSV
    echo "$t,$thr,$avg_lat" >> resultados.csv
done

rm temp_out.txt
echo "--------------------------------------"
echo "Concluído! Dados guardados em resultados.csv"
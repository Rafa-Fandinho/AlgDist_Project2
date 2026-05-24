#!/bin/bash
 
# =============================================================================
# benchmark.sh - Script completo de benchmark para Multi-Paxos / Raft
# Uso: bash benchmark.sh [protocolo] [payload]
#   protocolo: multi-paxos | raft  (default: multi-paxos)
#   payload:   tamanho do campo em bytes (default: 1000)
# =============================================================================
 
PROTO=${1:-multi-paxos}
PAYLOAD=${2:-1000}
SERVERS="127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002"
THREADS=(4 6 8 10 12 16 20 24 28 32 40 48 56 64 80 96 128 176 200)
 
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTDIR="results_${PROTO}_${TIMESTAMP}"
mkdir -p "$OUTDIR"
 
SUMMARY_CSV="$OUTDIR/summary.csv"
TIMESERIES_CSV="$OUTDIR/timeseries.csv"
 
# Cabeçalhos
echo "threads,throughput_ops_sec,avg_latency_ms,read_latency_ms,update_latency_ms" > "$SUMMARY_CSV"
echo "threads,elapsed_sec,ops_so_far,current_ops_sec,read_avg_lat_us,update_avg_lat_us" > "$TIMESERIES_CSV"
 
echo "=============================================="
echo " Benchmark: $PROTO | Payload: ${PAYLOAD}B"
echo " Resultados em: $OUTDIR"
echo "=============================================="
 
for t in "${THREADS[@]}"; do
    echo ""
    echo ">>> Testando $t thread(s)..."
 
    RAW_LOG="$OUTDIR/raw_t${t}.log"
 
    # Corre o cliente e guarda tudo (stdout + stderr)
    bash exec.sh "$t" "$PAYLOAD" "$SERVERS" 50 50 > "$RAW_LOG" 2>&1
 
    # --- Summary (resultados finais YCSB) ---
    thr=$(grep "\[OVERALL\], Throughput(ops/sec)" "$RAW_LOG" | cut -d',' -f3 | tr -d ' \r')
    read_lat=$(grep "\[READ\], AverageLatency(us)" "$RAW_LOG" | cut -d',' -f3 | tr -d ' \r')
    update_lat=$(grep "\[UPDATE\], AverageLatency(us)" "$RAW_LOG" | cut -d',' -f3 | tr -d ' \r')
 
    thr=${thr:-0}
    read_lat=${read_lat:-0}
    update_lat=${update_lat:-0}
 
    avg_lat=$(python3 -c "print(round((($read_lat + $update_lat) / 2.0) / 1000.0, 4))" 2>/dev/null || echo "0")
    read_ms=$(python3 -c "print(round($read_lat / 1000.0, 4))" 2>/dev/null || echo "0")
    update_ms=$(python3 -c "print(round($update_lat / 1000.0, 4))" 2>/dev/null || echo "0")
 
    echo "$t,$thr,$avg_lat,$read_ms,$update_ms" >> "$SUMMARY_CSV"
    echo "    Throughput: $thr ops/sec | Latência média: $avg_lat ms"
 
    # --- Time series (linhas de status periódico do YCSB) ---
    # Formato esperado: "... X sec: Y operations; Z current ops/sec; ... [READ AverageLatency(us)=A] ... [UPDATE AverageLatency(us)=B]"
    grep "current ops/sec" "$RAW_LOG" | while IFS= read -r line; do
        elapsed=$(echo "$line" | grep -oP '\d+ sec:' | grep -oP '\d+')
        ops=$(echo "$line" | grep -oP '\d+ operations' | grep -oP '\d+')
        cur_ops=$(echo "$line" | grep -oP '[\d.]+ current ops/sec' | grep -oP '[\d.]+')
        r_lat=$(echo "$line" | grep -oP 'READ AverageLatency\(us\)=[\d.]+' | grep -oP '[\d.]+$')
        u_lat=$(echo "$line" | grep -oP 'UPDATE AverageLatency\(us\)=[\d.]+' | grep -oP '[\d.]+$')
 
        elapsed=${elapsed:-0}
        ops=${ops:-0}
        cur_ops=${cur_ops:-0}
        r_lat=${r_lat:-0}
        u_lat=${u_lat:-0}
 
        echo "$t,$elapsed,$ops,$cur_ops,$r_lat,$u_lat" >> "$TIMESERIES_CSV"
    done
 
    # Pequena pausa entre runs para o sistema estabilizar
    sleep 5
done
 
echo ""
echo "=============================================="
echo " Benchmark concluído!"
echo " Summary:    $SUMMARY_CSV"
echo " Timeseries: $TIMESERIES_CSV"
echo " Logs raw:   $OUTDIR/raw_t*.log"
echo "=============================================="
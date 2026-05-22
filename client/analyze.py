#!/usr/bin/env python3
"""
analyze.py - Analisa os resultados do benchmark e gera gráficos + tabelas
Uso: python3 analyze.py <pasta_resultados> [pasta_resultados_2 ...]
Exemplo comparando dois protocolos:
    python3 analyze.py results_multi-paxos_20240101_120000 results_raft_20240101_130000
"""

import sys
import os
import csv
import re
from pathlib import Path

# Tenta importar matplotlib; se não tiver, avisa mas continua
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_PLOT = True
except ImportError:
    HAS_PLOT = False
    print("[AVISO] matplotlib não encontrado. Só serão geradas as tabelas CSV.")
    print("        Instala com: pip install matplotlib")

# ---------------------------------------------------------------------------
# Leitura de dados
# ---------------------------------------------------------------------------

def read_summary(folder):
    path = Path(folder) / "summary.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "threads":       int(row["threads"]),
                "throughput":    float(row["throughput_ops_sec"]),
                "avg_lat_ms":    float(row["avg_latency_ms"]),
                "read_lat_ms":   float(row["read_latency_ms"]),
                "update_lat_ms": float(row["update_latency_ms"]),
            })
    return rows

def read_timeseries(folder):
    path = Path(folder) / "timeseries.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "threads":     int(row["threads"]),
                "elapsed_sec": int(row["elapsed_sec"]),
                "ops":         int(row["ops_so_far"]),
                "cur_ops_sec": float(row["current_ops_sec"]),
                "read_us":     float(row["read_avg_lat_us"]),
                "update_us":   float(row["update_avg_lat_us"]),
            })
    return rows

def label_for(folder):
    """Extrai um label legível do nome da pasta."""
    name = Path(folder).name
    if "multi-paxos" in name or "multipaxos" in name:
        return "Multi-Paxos"
    if "raft" in name:
        return "Raft"
    return name

# ---------------------------------------------------------------------------
# Geração de tabelas CSV melhoradas
# ---------------------------------------------------------------------------

def write_comparison_table(datasets, outdir):
    """Gera uma tabela única com todos os protocolos lado a lado."""
    path = Path(outdir) / "comparison_table.csv"
    
    # Recolhe todos os valores de threads
    all_threads = sorted(set(r["threads"] for d in datasets.values() for r in d))
    
    with open(path, "w", newline="") as f:
        protos = list(datasets.keys())
        header = ["threads"]
        for p in protos:
            header += [f"{p}_throughput", f"{p}_avg_lat_ms", f"{p}_read_lat_ms", f"{p}_update_lat_ms"]
        f.write(",".join(header) + "\n")
        
        for t in all_threads:
            row = [str(t)]
            for p in protos:
                match = next((r for r in datasets[p] if r["threads"] == t), None)
                if match:
                    row += [
                        f"{match['throughput']:.2f}",
                        f"{match['avg_lat_ms']:.4f}",
                        f"{match['read_lat_ms']:.4f}",
                        f"{match['update_lat_ms']:.4f}",
                    ]
                else:
                    row += ["", "", "", ""]
            f.write(",".join(row) + "\n")
    
    print(f"[OK] Tabela comparativa: {path}")
    return path

# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------

COLORS = ["#2563eb", "#dc2626", "#16a34a", "#d97706", "#7c3aed"]
MARKERS = ["o", "s", "^", "D", "v"]

def plot_throughput_latency(datasets, outdir):
    """Curva throughput vs latência (o gráfico principal do paper)."""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    for i, (label, rows) in enumerate(datasets.items()):
        xs = [r["throughput"] for r in rows]
        ys = [r["avg_lat_ms"] for r in rows]
        threads = [r["threads"] for r in rows]
        
        color = COLORS[i % len(COLORS)]
        ax.plot(xs, ys, marker=MARKERS[i % len(MARKERS)], color=color,
                label=label, linewidth=2, markersize=6)
        
        # Anota o número de threads em alguns pontos
        for x, y, t in zip(xs, ys, threads):
            if t in (1, 4, 16, 64, 128):
                ax.annotate(f"{t}t", (x, y), textcoords="offset points",
                            xytext=(5, 5), fontsize=7, color=color)
    
    ax.set_xlabel("Throughput (ops/sec)", fontsize=12)
    ax.set_ylabel("Latência Média (ms)", fontsize=12)
    ax.set_title("Throughput vs Latência", fontsize=13, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    
    plt.tight_layout()
    out = Path(outdir) / "throughput_latency.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Gráfico throughput vs latência: {out}")

def plot_throughput_vs_threads(datasets, outdir):
    """Throughput em função do número de threads."""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    for i, (label, rows) in enumerate(datasets.items()):
        xs = [r["threads"] for r in rows]
        ys = [r["throughput"] for r in rows]
        ax.plot(xs, ys, marker=MARKERS[i % len(MARKERS)],
                color=COLORS[i % len(COLORS)], label=label, linewidth=2, markersize=6)
    
    ax.set_xlabel("Número de Threads (clientes)", fontsize=12)
    ax.set_ylabel("Throughput (ops/sec)", fontsize=12)
    ax.set_title("Throughput por Número de Threads", fontsize=13, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    
    plt.tight_layout()
    out = Path(outdir) / "throughput_vs_threads.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Gráfico throughput vs threads: {out}")

def plot_latency_vs_threads(datasets, outdir):
    """Latência em função do número de threads."""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    for i, (label, rows) in enumerate(datasets.items()):
        xs = [r["threads"] for r in rows]
        ys = [r["avg_lat_ms"] for r in rows]
        ax.plot(xs, ys, marker=MARKERS[i % len(MARKERS)],
                color=COLORS[i % len(COLORS)], label=label, linewidth=2, markersize=6)
    
    ax.set_xlabel("Número de Threads (clientes)", fontsize=12)
    ax.set_ylabel("Latência Média (ms)", fontsize=12)
    ax.set_title("Latência por Número de Threads", fontsize=13, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    out = Path(outdir) / "latency_vs_threads.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Gráfico latência vs threads: {out}")

def plot_timeseries(ts_data_map, outdir):
    """Throughput ao longo do tempo para cada contagem de threads."""
    # Agrupa por protocolo e threads
    all_threads = sorted(set(r["threads"] for rows in ts_data_map.values() for r in rows))
    
    for t in all_threads:
        fig, ax = plt.subplots(figsize=(8, 4))
        found = False
        
        for i, (label, rows) in enumerate(ts_data_map.items()):
            subset = [r for r in rows if r["threads"] == t]
            if not subset:
                continue
            found = True
            xs = [r["elapsed_sec"] for r in subset]
            ys = [r["cur_ops_sec"] for r in subset]
            ax.plot(xs, ys, color=COLORS[i % len(COLORS)],
                    label=label, linewidth=2, marker=".")
        
        if not found:
            plt.close(fig)
            continue
        
        ax.set_xlabel("Tempo (seg)", fontsize=11)
        ax.set_ylabel("Throughput instantâneo (ops/sec)", fontsize=11)
        ax.set_title(f"Throughput ao longo do tempo — {t} thread(s)", fontsize=12, fontweight="bold")
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        
        plt.tight_layout()
        out = Path(outdir) / f"timeseries_t{t:03d}.png"
        fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
    
    print(f"[OK] Gráficos timeseries guardados em {outdir}/")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    folders = sys.argv[1:]
    outdir = "analysis_" + "_".join(Path(f).name[:20] for f in folders)
    Path(outdir).mkdir(exist_ok=True)
    
    summary_data = {}
    ts_data = {}
    
    for folder in folders:
        label = label_for(folder)
        # Se dois protocolos tiverem o mesmo label, diferencia pelo timestamp
        if label in summary_data:
            label = label + "_2"
        
        try:
            summary_data[label] = read_summary(folder)
            print(f"[OK] Summary lido: {folder} → {label} ({len(summary_data[label])} pontos)")
        except FileNotFoundError:
            print(f"[ERRO] summary.csv não encontrado em {folder}")
            continue
        
        try:
            ts_data[label] = read_timeseries(folder)
            print(f"[OK] Timeseries lido: {folder} → {len(ts_data[label])} linhas")
        except FileNotFoundError:
            print(f"[AVISO] timeseries.csv não encontrado em {folder} — skipping")
    
    if not summary_data:
        print("Nenhum dado para analisar.")
        sys.exit(1)
    
    # Tabela comparativa
    write_comparison_table(summary_data, outdir)
    
    if HAS_PLOT:
        plot_throughput_latency(summary_data, outdir)
        plot_throughput_vs_threads(summary_data, outdir)
        plot_latency_vs_threads(summary_data, outdir)
        if ts_data:
            plot_timeseries(ts_data, outdir)
    
    print(f"\nAnálise completa! Resultados em: {outdir}/")

if __name__ == "__main__":
    main()
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import csv

consumers = [1, 3, 5, 8]
colores = {1: "green", 3: "blue", 5: "orange", 8: "darkred"}

os.makedirs("resultados/graficos", exist_ok=True)

fig, ax = plt.subplots(figsize=(10, 6))

for c in consumers:
    path = f"resultados/lag_historico_spike_{c}consumers.csv"
    if not os.path.exists(path):
        print(f"  [ADVERTENCIA] No existe: {path}")
        continue

    ts, lags = [], []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts.append(int(row["t"]))
                lags.append(int(row["lag"]))
            except:
                pass

    ax.plot(ts, lags, color=colores[c], linewidth=2, label=f"{c} consumer{'s' if c > 1 else ''}")

# Marcar dónde ocurre el spike (N_PEDIDOS/2 * DELAY_MS / 1000)
# Con N_PEDIDOS=2500 y DELAY_MS=5 → spike en t=6.25s
# Pero en tiempo real desde inicio del generador ~6s después de arrancar

# Spike empieza en t ≈ 12s (1250 consultas × 5ms + ~5s arranque)
ax.axvline(x=12, color="gray", linestyle="--", alpha=0.6, label="Inicio spike")

# Spike dura 500 consultas × 1ms = 0.5s + 1250 × 5ms = 6.25s más
ax.axvline(x=19, color="gray", linestyle=":",  alpha=0.6, label="Fin spike")

ax.set_xlabel("Segundos desde inicio de captura", fontsize=12)
ax.set_ylabel("Lag tópico consultas", fontsize=12)
ax.set_title("Backlog durante Spike de Tráfico por número de consumers", fontsize=13)
ax.legend(fontsize=10)
ax.grid(axis="y", alpha=0.4)

path_out = "resultados/graficos/e6_lag_spike.png"
fig.savefig(path_out, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"✔ Guardado: {path_out}")
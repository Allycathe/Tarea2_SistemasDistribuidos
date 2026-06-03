import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import csv

tiempos = [10, 30, 50]
colores = {10: "green", 30: "blue", 50: "darkred"}

os.makedirs("resultados/graficos", exist_ok=True)

fig, ax = plt.subplots(figsize=(10, 6))

for tf in tiempos:
    path = f"resultados/lag_historico_{tf}s.csv"
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

    ax.plot(ts, lags, color=colores[tf], linewidth=2, label=f"{tf}s")

ax.set_xlabel("Segundos desde inicio de captura", fontsize=12)
ax.set_ylabel("Lag tópico consultas", fontsize=12)
ax.set_title("Backlog de consultas durante caídas temporales", fontsize=13)
ax.legend(fontsize=11)
ax.grid(axis="y", alpha=0.4)

path_out = "resultados/graficos/e4_lag_historico.png"
fig.savefig(path_out, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"✔ Guardado: {path_out}")
# Tarea 1: sistemas distribuidos
**Autores**: Enzo Rodriguez y Alonso Iturra
Este proyecto es un simulador de caché que evalúa el rendimiento de diferentes tamaños de cache y políticas de reemplazo (**LFU** y **LRU**), siguiendo 2 tipos de distribuciones de consultas (Uniforme y Zipf).
Antes de empezar, es necesario tener:
-  Git
-  Docker
-  Bash
## Paso 1: Clonar el repositorio
En cualquier carpeta correr:
```bash
git clone https://github.com/HonryQuinn/Tarea1_SistemasDistribuidos.git
cd Tarea1_SistemasDistribuidos/
``` 
## Paso 2: Añadir dataset
Una vez descargado el repositorio se debería ver la siguiente estructura:
 ```
.
├── dataset
│   └── buildings.csv  <-- Aquí debe ir el dataset!
├── docker-compose.yml
├── metricas
│   ├── Dockerfile
│   └── metricas.py
├── respuestas
│   ├── data
│   ├── Dockerfile
│   └── engine.py
├── resultados   <-- Aquí se generan los reportes
├── run.sh
└── trafico
    ├── Dockerfile
    └── main.py
```
**Importante**: el dataset debe estar descomprimido y en la ruta dataset, debería verse como en la estructura anterior.
El código avisa si encuentra o no el dataset
## Paso 3: Correr simulación
Para iniciar el proceso de pruebas automáticas, ejecuta el script principal con privilegios de administrador:
```bash
sudo bash run.sh
```
Simplemente dejé correr el script, automaticamente generará un txt con los resultados obtenidos para cada configuración y distribución
# Resultados esperados
El código mostrará una tabla con las siguientes métricas:
- Hit rate: Porcentaje de aciertos en la caché.
- Latencia p50/95: Tiempo de respuesta mediano y del percentil 95.
- Throughput: Consultas procesadas por segundo (qps)
- Eviction rate: Tasa de expulsión de elementos por minuto.
# Componentes del sistema
- Redis: es el caché.
- Tráfico: se encarga de realizar 100.000 consultas para la distribución uniforme y Zipf
- Sistema de respuestas: en caso de haber miss, est se encargará de calcular la consulta. 
- Sistetma de métricas: encargado de registrar las métricas.

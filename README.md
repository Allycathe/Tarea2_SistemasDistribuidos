# Tarea 2: sistemas distribuidos
**Autores**: Enzo Rodriguez y Alonso Iturra
Este proyecto es un sistema con procesamiento de consultas y Fallback con Apache Kafka en Internet, el cual evalúa el rendimiento de diferentes métricas en distintos escenarios posbiles dentro de la realidad.
Antes de empezar, es necesario tener:
-  Git
-  Docker
-  Kafka
-  Zookeeper
-  Bash
## Paso 1: Clonar el repositorio
En cualquier carpeta correr:
```bash
git clone https://github.com/HonryQuinn/Tarea2_SistemasDistribuidos.git
cd Tarea2_SistemasDistribuidos/
``` 
## Paso 2: Añadir dataset
Una vez descargado el repositorio se debería ver la siguiente estructura:
 ```
.
├── consumidores
│   ├── dlq.py
│   ├── Dockerfile
│   ├── principal.py
│   └── reintentos.py
├── dataset
│   └── buildings.csv
├── docker-compose.yml
├── metricas
│   ├── Dockerfile
│   └── metricas.py
├── ps
├── README.md
├── respuestas
│   ├── Dockerfile
│   └── engine.py
├── resultados
├── run.sh
├── test_e5.sh
└── trafico
    ├── Dockerfile
    └── main.py

```
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

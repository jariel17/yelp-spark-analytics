# Proyecto Final Big Data — Dataset Yelp

Proyecto final de Big Data y Administración de Datos,  
Maestría en Analítica de Datos, Universidad Tecnológica de Panamá

---

## Requisitos previos

- Python 3.11+ con [uv](https://github.com/astral-sh/uv) instalado
- Java 11 o 17 (requerido por Apache Spark)
- Credenciales de los clusters configuradas en `.env` (ver `.env.example`)

Instalar dependencias:

```bash
uv sync
```

## Dataset: Yelp Open Dataset

Dataset público de Yelp con datos reales de negocios, reseñas, usuarios y actividad. 

### Archivos originales

| Archivo | Registros | Tamaño aprox. | Descripción |
|---|---|---|---|
| `business.json` | 150,346 | ~100 MB | Negocios: ubicación, rating, atributos, categorías, horarios |
| `checkin.json` | 131,930 | ~70 MB | Registros de visitas por negocio (timestamps como string) |
| `tip.json` | 908,915 | ~200 MB | Sugerencias cortas de usuarios sobre negocios |
| `review.json` | ~7,000,000 | ~5.5 GB | Reseñas completas |
| `user.json` | ~2,000,000 | ~2.0 GB | Perfil de usuarios incluyendo lista de amigos|

### Obtener el dataset
1. Ir a [https://www.yelp.com/dataset](https://www.yelp.com/dataset)
2. Descargar el archivo `yelp_dataset.tar` y descomprimirlo.
3. Copiar los siguientes archivos JSON a la carpeta `data/raw`:

```
data/
└── raw/
    ├── yelp_academic_dataset_business.json
    ├── yelp_academic_dataset_checkin.json
    ├── yelp_academic_dataset_review.json
    ├── yelp_academic_dataset_tip.json
    └── yelp_academic_dataset_user.json

```

> Los archivos JSON **no se incluyen en el repositorio** por su tamaño (~8 GB el dataset completo)
> y por los términos de uso de Yelp.



---

## Configurar credenciales

Copiar el archivo de ejemplo y completar con las credenciales reales:

```bash
cp .env.example .env
```

Editar `.env` con las cadenas de conexión de CockroachDB, Aiven PostgreSQL y MongoDB Atlas.

---

## ETL

El dataset público de Yelp en su estado original pesa 8.65 GB y contiene millones de registros que abarcan desde 2004 hasta 2022. Procesar ese volumen completo con recursos locales (un equipo con 4–8 GB de RAM disponibles para Spark) sería inviable, y subir 8.65 GB a servicios de nube en el free tier de MongoDB Atlas, Aiven o CockroachDB resultaría en costos inesperados o errores de cuota.

### Visión general del proceso

El pipeline se divide en **dos fases claramente separadas**, coordinadas por un
orquestador central:

```
run_pipeline.py  (orquestador)
    │
    ├─── [Fase 1: Transformación]
    │        pipeline.py
    │        Entrada : data/raw/          (archivos JSON originales)
    │        Salida  : data/raw_trimmed/  (campos recortados)
    │                  data/filtered_trimmed/  (dataset final limpio)
    │
    └─── [Fase 2: Carga]
             load_business_mongo.py  → MongoDB Atlas cluster 1  (colección business)
             load_checkin_mongo.py   → MongoDB Atlas cluster 2  (colección checkins)
             load_tip_mongo.py       → MongoDB Atlas cluster 2  (colección tips)
             load_user_crdb.py       → CockroachDB    (tabla users)
             load_review_aiven.py    → Aiven PostgreSQL (tabla reviews)
```

La fase de transformación produce dos capas de datos intermedios:

| Directorio | Contenido | Propósito |
|---|---|---|
| `data/raw/` | JSON originales sin modificar | Fuente de verdad; nunca se modifica |
| `data/raw_trimmed/` | JSON con campos eliminados (business y user) | Paso intermedio — solo reducción de columnas |
| `data/filtered_trimmed/` | Dataset final: filtrado temporalmente, muestreado, con integridad referencial | Entrada para las cargas a la nube y para las analíticas |

### Extracción y transformación: `pipeline.py`

`pipeline.py` realiza el siguiente proceso de transformación en nueve pasos, aplicados a los archivos JSON originales:

```
RAW (8.65 GB)
  │
  ├─ [1] Recorte de campos: user.json   →  raw_trimmed/user.json
  ├─ [2] Recorte de campos: business.json  →  raw_trimmed/business.json
  │         (menos columnas, mismo # de filas)
  │
  ├─ [3] Muestreo de usuarios (40%)  →  filtered_trimmed/user.json
  │         ↓ produce: conjunto de user_ids válidos en memoria
  │
  ├─ [4] Filtro en cascada: reviews   →  filtered_trimmed/review.json
  ├─ [5] Filtro en cascada: tips      →  filtered_trimmed/tip.json
  │         (solo registros cuyo user_id está en el conjunto)
  │
  ├─ [6] Filtro temporal 2018–2021: reviews  →  modifica in-place
  ├─ [7] Filtro temporal 2018–2021: tips     →  modifica in-place
  ├─ [8] Filtro temporal 2018–2021: checkins →  filtered_trimmed/checkin.json
  │
  └─ [9] Muestreo de reviews (20%)  →  modifica in-place
              ↓
         filtered_trimmed/  (dataset final ~708 MB)
```

#### Descripción de cada paso

**[1] y [2] — Recorte de campos**
Se eliminan columnas que no aportan valor analítico para reducir el tamaño de los archivos antes de cualquier otro procesamiento.

| Archivo | Campos eliminados | Motivo |
|---|---|---|
| `user.json` | `friends` | Lista masiva de IDs de amigos sin uso analítico |
| `user.json` | `compliment_*` (9 campos) | Contadores de aplausos de poca relevancia para el análisis |
| `business.json` | `address`, `postal_code`, `latitude`, `longitude` | Datos de ubicación exacta irrelevantes para las consultas |

**[3] — Muestreo de usuarios (40%)**
Con ~2 millones de usuarios en el dataset original, procesarlos todos superaría los límites del free tier en la nube. Se selecciona aleatoriamente el 40% usando una semilla fija (`seed=42`) para garantizar que el resultado sea reproducible. Los `user_ids` seleccionados quedan en memoria y guían los pasos siguientes.

**[4] y [5] — Filtro en cascada**
Reviews y tips contienen un campo `user_id` que los vincula a usuarios. Solo se conservan los registros cuyo `user_id` pertenece al subconjunto muestreado en el paso anterior, manteniendo la integridad referencial entre colecciones.

**[6], [7] y [8] — Filtro temporal 2018–2021**
Se descarta toda actividad fuera del período de estudio. Reviews y tips se filtran in-place (sobreescribiendo el archivo con un reemplazo atómico). Checkins tienen un formato especial: el campo `date` es un string con múltiples timestamps separados por coma, por lo que el filtrado opera timestamp a timestamp dentro de cada registro.

**[9] — Muestreo de reviews (20%)**
Incluso después del filtrado, reviews sigue siendo el archivo más pesado. Se aplica un segundo muestreo para reducirlo a un volumen manejable para la carga a la nube y los análisis locales con Spark.

### La fase de carga: los cinco loaders

Una vez que `pipeline.py` produce el dataset limpio en `data/filtered_trimmed/`,
los cinco scripts de carga distribuyen cada colección a su destino en la nube.

Todos los scripts de carga comparten una arquitectura idéntica:

```
1. Cargar credenciales desde .env (via python-dotenv)
2. Verificar flag ALREADY_LOADED → termina la ejecución si true
3. Establecer conexión al servicio de destino
4. Crear colección/tabla si no existe (DDL)
5. Leer el archivo fuente línea por línea (streaming)
6. Acumular registros en un batch de 500
7. Cuando el batch se llena → enviar a la BD → vaciar batch
8. Enviar el batch final (residual < 500 registros)
9. Verificar el conteo en la BD
10. Imprimir resumen y cerrar conexión
```

#### ¿Por qué tres bases de datos distintas?

La distribución de colecciones entre MongoDB Atlas, CockroachDB y Aiven
PostgreSQL es intencional. Cada colección tiene características de datos distintas que se adaptan mejor a un tipo específico de base de datos:

| Colección | BD | Justificación |
|---|---|---|
| `business` | MongoDB Atlas | Contiene campos altamente anidados e irregulares (`attributes`, `hours`). MongoDB almacena documentos JSON nativamente sin necesidad de aplanar la estructura. |
| `checkins`, `tips` | MongoDB Atlas | Son eventos transaccionales semiestructurados donde no todos los registros tienen los mismos campos. |
| `users` | CockroachDB | Perfil de usuario con estructura tabular plana y estable. CockroachDB ofrece el protocolo PostgreSQL con escalabilidad horizontal. |
| `reviews` | Aiven PostgreSQL | Datos tabulares con esquema fijo y texto. PostgreSQL es una base de datos relacional madura para análisis SQL. |

### El orquestador: `run_pipeline.py`
`run_pipeline.py` es el punto de entrada único del ETL. Su función es ejecutar los
seis scripts que componen el pipeline **en el orden correcto** y **detener la ejecución
inmediatamente si alguno falla**.


#### ¿Por qué separar la transformación de la carga?

La separación entre `pipeline.py` y los scripts de carga permite:

1. **Reejecutar las cargas independientemente** sin volver a transformar los datos
   (que es el paso más costoso en tiempo).
2. **Verificar el dataset intermedio** inspeccionando los archivos en
   `data/filtered_trimmed/` antes de subirlos a la nube.
3. **Minimizar el consumo del free tier** al poder saltar cargas ya completadas sin necesidad de modificar el código.

## Analíticas — Ejecutar las 10 consultas SparkSQL

```bash
uv run jupyter notebook analytics/consultas_book.ipynb
```

### SparkSession

Antes de ejecutar cualquier consulta, el notebook inicializa una `SparkSession`, que es el punto de entrada a todas las funcionalidades de Spark. Cada parámetro tiene un propósito específico:

```python
spark = SparkSession.builder \
    .appName('YelpBigData-Notebook') \
    .master('local[*]') \
    .config('spark.driver.memory', '4g') \
    .config('spark.sql.shuffle.partitions', '8') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
    .config('spark.jars.packages', '...') \
    .getOrCreate()
```

| Parámetro | Valor | Significado |
|---|---|---|
| `appName` | `YelpBigData-Notebook` | Nombre del trabajo |
| `master` | `local[*]` | Ejecuta Spark en modo local usando todos los núcleos disponibles de la máquina |
| `spark.driver.memory` | `4g` | Memoria asignada al proceso principal (driver) que coordina los workers |
| `spark.sql.shuffle.partitions` | `8` | Número de particiones al reorganizar datos (shuffle). |
| `spark.sql.adaptive.enabled` | `true` | Activa la ejecución adaptativa: Spark ajusta el plan de ejecución en tiempo real según el volumen real de los datos |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Reduce automáticamente el número de particiones al final de un shuffle si los datos resultantes son pequeños, evitando overhead innecesario |
| `spark.jars.packages` | mongo-spark-connector + postgresql | Descarga automáticamente los conectores JDBC para MongoDB y PostgreSQL, necesarios para el modo clusters |

### Carga de datos

El notebook soporta dos modos de carga, seleccionables en la celda final:

#### Modo local

Lee los archivos JSON directamente desde `data/filtered_trimmed/`. No requiere credenciales ni conexión de red.

```python
cargar_vistas_local()
```

#### Modo clusters (demuestra la arquitectura distribuida)

Lee los datos directamente desde los tres servicios en la nube. Requiere las credenciales `_READ` configuradas en `.env`.

```python
cargar_vistas_clusters()
```

| Vista Spark | Fuente en la nube |
|---|---|
| `business` | MongoDB Atlas |
| `tips` | MongoDB Atlas |
| `checkins` | MongoDB Atlas |
| `reviews` | Aiven PostgreSQL (JDBC) |
| `users` | CockroachDB (JDBC) |

Los gráficos generados se guardan automáticamente en la carpeta `graficos/`
como archivos PNG (`q01_*.png` … `q10_*.png`).

---

### Descripción de las 10 consultas

#### Q01 — Índice de oportunidad de mercado por ciudad
Calcula un puntaje compuesto por ciudad combinando el rating promedio, el porcentaje de negocios aún abiertos y el volumen total de reseñas. **Valor de negocio:** permite identificar en qué ciudades existe mayor potencial para abrir o invertir en un negocio, priorizando mercados con demanda probada, negocios viables y buena percepción de calidad.

#### Q02 — Franjas horarias y tráfico de check-ins
Clasifica los check-ins en cuatro franjas del día (mañana, tarde, noche, madrugada) y determina qué negocios concentran más visitas en cada franja, por ciudad. **Valor de negocio:** ayuda a tomar decisiones de horario, staffing y promociones según cuándo fluye realmente el tráfico en cada mercado.

#### Q03 — Categorías mejor valoradas con volumen significativo
Identifica las categorías de negocio con el mejor rating promedio, exigiendo un mínimo de 1,000 reseñas para que el resultado sea estadísticamente representativo. **Valor de negocio:** guía la selección de nicho al mostrar qué rubros logran consistentemente alta satisfacción del cliente a escala.

#### Q04 — Precio vs. rating promedio por categoría
Cruza el rango de precio de los negocios (atributo `RestaurantsPriceRange2`) con su rating promedio, segmentado por categoría. **Valor de negocio:** revela si los clientes perciben valor en cada nivel de precio y qué categorías ofrecen la mejor relación calidad-precio.

#### Q05 — Ranking de líderes de mercado por categoría
Aplica `DENSE_RANK` para identificar los tres negocios más destacados dentro de cada categoría, ordenados por volumen de reseñas y rating. **Valor de negocio:** mapea quiénes son los referentes en cada nicho, lo cual sirve de benchmark competitivo para negocios que quieren entender contra quién compiten.

#### Q06 — Usuarios más influyentes por impacto de reseñas
Construye un índice de influencia ponderando el volumen de reseñas escritas, la diversidad de negocios evaluados y el total de votos útiles recibidos. **Valor de negocio:** identifica a los reviewers cuyas opiniones tienen mayor alcance real en la plataforma, útil para estrategias de marketing de influencia o para ponderar reseñas en sistemas de recomendación.

#### Q07 — Impacto de la pandemia y recuperación por estado (2018–2021)
Calcula el volumen de reseñas por año y estado, y mide el cambio porcentual interanual usando la función de ventana `LAG`. **Valor de negocio:** cuantifica el golpe del COVID-19 sobre la actividad en Yelp por estado y permite comparar qué mercados se recuperaron más rápido.

#### Q08 — Negocios con alto tráfico pero baja satisfacción
Combina datos de check-ins y reseñas para detectar negocios que atraen mucho público (top 10% de tráfico en su estado) pero que tienen baja calificación (bottom 25% de rating). **Valor de negocio:** estos negocios representan una oportunidad o una alerta: son lugares que logran atraer clientes pero no los satisfacen, señal de problemas operativos que, si se corrigen, pueden convertirse en negocios de alto rendimiento.

#### Q09 — Tendencias de calidad y brecha de rendimiento mensual
Calcula el rating mensual de cada negocio y lo compara contra su propia media móvil de 3 meses usando una ventana deslizante. **Valor de negocio:** detecta caídas abruptas de calidad en negocios específicos, lo que puede indicar cambios de management, problemas de servicio o eventos puntuales. Es un indicador de alerta temprana para gestión de calidad.

#### Q10 — Clustering de negocios por segmentación (K-Means)
Aplica el algoritmo K-Means con 4 clusters sobre las variables `stars`, `review_count` y `total_checkins`, normalizadas con `StandardScaler`. **Valor de negocio:** agrupa los negocios en segmentos naturales según su perfil de popularidad y calidad, sin etiquetas predefinidas. Permite a plataformas como Yelp personalizar la experiencia por tipo de negocio o diseñar estrategias diferenciadas para cada segmento.

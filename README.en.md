# Big Data Final Project — Yelp Dataset

Final project for Big Data and Data Administration,  
Master's in Data Analytics, Universidad Tecnológica de Panamá

---

## Prerequisites

- Python 3.11+ with [uv](https://github.com/astral-sh/uv) installed
- Java 11 or 17 (required by Apache Spark)
- Cluster credentials configured in `.env` (see `.env.example`)

Install dependencies:

```bash
uv sync
```

## Dataset: Yelp Open Dataset

A public Yelp dataset with real-world data on businesses, reviews, users, and activity.

### Original files

| File | Records | Approx. size | Description |
|---|---|---|---|
| `business.json` | 150,346 | ~100 MB | Businesses: location, rating, attributes, categories, hours |
| `checkin.json` | 131,930 | ~70 MB | Visit records per business (timestamps as a string) |
| `tip.json` | 908,915 | ~200 MB | Short user suggestions about businesses |
| `review.json` | ~7,000,000 | ~5.5 GB | Full reviews |
| `user.json` | ~2,000,000 | ~2.0 GB | User profiles including friend lists |

### Getting the dataset
1. Go to [https://www.yelp.com/dataset](https://www.yelp.com/dataset)
2. Download `yelp_dataset.tar` and extract it.
3. Copy the following JSON files into the `data/raw` folder:

```
data/
└── raw/
    ├── yelp_academic_dataset_business.json
    ├── yelp_academic_dataset_checkin.json
    ├── yelp_academic_dataset_review.json
    ├── yelp_academic_dataset_tip.json
    └── yelp_academic_dataset_user.json
```

> The JSON files are **not included in this repository** due to their size (~8 GB total)
> and Yelp's terms of use.

---

## Configuring credentials

Copy the example file and fill in the real credentials:

```bash
cp .env.example .env
```

Edit `.env` with the connection strings for CockroachDB, Aiven PostgreSQL, and MongoDB Atlas.

---

## ETL

The raw Yelp dataset weighs 8.65 GB and spans millions of records from 2004 to 2022. Processing the full volume on local hardware (a machine with 4–8 GB of RAM available for Spark) is not feasible, and uploading 8.65 GB to cloud services on the free tier of MongoDB Atlas, Aiven, or CockroachDB would trigger unexpected costs or quota errors.

### Process overview

The pipeline is split into **two clearly separated phases**, coordinated by a central orchestrator:

```
run_pipeline.py  (orchestrator)
    │
    ├─── [Phase 1: Transformation]
    │        pipeline.py
    │        Input  : data/raw/          (original JSON files)
    │        Output : data/raw_trimmed/  (fields removed)
    │                 data/filtered_trimmed/  (final clean dataset)
    │
    └─── [Phase 2: Load]
             load_business_mongo.py  → MongoDB Atlas cluster 1  (business collection)
             load_checkin_mongo.py   → MongoDB Atlas cluster 2  (checkins collection)
             load_tip_mongo.py       → MongoDB Atlas cluster 2  (tips collection)
             load_user_crdb.py       → CockroachDB    (users table)
             load_review_aiven.py    → Aiven PostgreSQL (reviews table)
```

The transformation phase produces two intermediate data layers:

| Directory | Contents | Purpose |
|---|---|---|
| `data/raw/` | Original unmodified JSONs | Source of truth; never touched |
| `data/raw_trimmed/` | JSONs with fields removed (business and user) | Intermediate step — column reduction only |
| `data/filtered_trimmed/` | Final dataset: time-filtered, sampled, referentially consistent | Input for cloud uploads and analytics |

### Extraction and transformation: `pipeline.py`

`pipeline.py` runs the following transformation process in nine steps, applied to the original JSON files:

```
RAW (8.65 GB)
  │
  ├─ [1] Field trim: user.json      →  raw_trimmed/user.json
  ├─ [2] Field trim: business.json  →  raw_trimmed/business.json
  │         (fewer columns, same row count)
  │
  ├─ [3] User sampling (40%)  →  filtered_trimmed/user.json
  │         ↓ produces: set of valid user_ids in memory
  │
  ├─ [4] Cascade filter: reviews  →  filtered_trimmed/review.json
  ├─ [5] Cascade filter: tips     →  filtered_trimmed/tip.json
  │         (only records whose user_id is in the set)
  │
  ├─ [6] Time filter 2018–2021: reviews  →  modified in-place
  ├─ [7] Time filter 2018–2021: tips     →  modified in-place
  ├─ [8] Time filter 2018–2021: checkins →  filtered_trimmed/checkin.json
  │
  └─ [9] Review sampling (20%)  →  modified in-place
              ↓
         filtered_trimmed/  (final dataset ~708 MB)
```

#### Step-by-step breakdown

**[1] and [2] — Field trimming**
Columns that carry no analytical value are dropped before any further processing, reducing file size upfront.

| File | Dropped fields | Reason |
|---|---|---|
| `user.json` | `friends` | Massive list of friend IDs with no analytical use |
| `user.json` | `compliment_*` (9 fields) | Applause counters of little relevance to the analysis |
| `business.json` | `address`, `postal_code`, `latitude`, `longitude` | Exact location data irrelevant to the queries |

**[3] — User sampling (40%)**
With ~2 million users in the original dataset, loading all of them would exceed cloud free-tier limits. 40% are selected at random using a fixed seed (`seed=42`) to ensure the result is fully reproducible. The selected `user_ids` are held in memory and drive the next steps.

**[4] and [5] — Cascade filter**
Reviews and tips each contain a `user_id` that links them to a user. Only records whose `user_id` belongs to the sampled subset are kept, preserving referential integrity across collections.

**[6], [7] and [8] — Time filter 2018–2021**
All activity outside the study period is discarded. Reviews and tips are filtered in-place via an atomic file replace. Checkins have a special format: the `date` field is a single comma-separated string of timestamps, so filtering happens timestamp by timestamp within each record rather than record by record.

**[9] — Review sampling (20%)**
Even after filtering, reviews remains the heaviest file. A second sampling pass brings it down to a volume that is manageable for both the cloud upload and local Spark analysis.

### The load phase: the five loaders

Once `pipeline.py` produces the clean dataset in `data/filtered_trimmed/`, the five loader scripts distribute each collection to its cloud destination.

All loaders share the same architecture:

```
1. Load credentials from .env (via python-dotenv)
2. Check ALREADY_LOADED flag → exit early if true
3. Open connection to the target service
4. Create collection/table if it doesn't exist (DDL)
5. Read the source file line by line (streaming)
6. Accumulate records in a batch of 500
7. When the batch is full → send to the DB → clear batch
8. Send the final batch (remainder < 500 records)
9. Verify the count in the DB
10. Print summary and close connection
```

#### Why three different databases?

The split across MongoDB Atlas, CockroachDB, and Aiven PostgreSQL is intentional. Each collection has distinct data characteristics that map better to a specific database type:

| Collection | DB | Rationale |
|---|---|---|
| `business` | MongoDB Atlas | Contains highly nested and irregular fields (`attributes`, `hours`). MongoDB stores JSON documents natively without needing to flatten the structure. |
| `checkins`, `tips` | MongoDB Atlas | Semi-structured transactional events where not every record shares the same fields. |
| `users` | CockroachDB | Flat, stable tabular profile data. CockroachDB speaks the PostgreSQL protocol with horizontal scalability. |
| `reviews` | Aiven PostgreSQL | Tabular data with a fixed schema and text. PostgreSQL is a mature relational database well-suited for SQL analytics. |

### The orchestrator: `run_pipeline.py`
`run_pipeline.py` is the single entry point for the entire ETL. Its job is to run the six pipeline scripts **in the correct order** and **stop immediately if any one of them fails**.

#### Why separate transformation from loading?

Keeping `pipeline.py` and the loader scripts separate allows you to:

1. **Re-run the loaders independently** without re-transforming the data (which is the most time-expensive step).
2. **Inspect the intermediate dataset** by checking the files in `data/filtered_trimmed/` before pushing anything to the cloud.
3. **Minimize free-tier consumption** by skipping already-completed loads without touching the code.

---

## Analytics — Running the 10 SparkSQL queries

```bash
uv run jupyter notebook analytics/consultas_book.ipynb
```

### SparkSession

Before any query runs, the notebook initializes a `SparkSession` — the entry point to all Spark functionality. Each parameter serves a specific purpose:

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

| Parameter | Value | What it does |
|---|---|---|
| `appName` | `YelpBigData-Notebook` | Job name shown in the Spark UI |
| `master` | `local[*]` | Runs Spark in local mode using all available CPU cores |
| `spark.driver.memory` | `4g` | Memory allocated to the driver process that coordinates execution |
| `spark.sql.shuffle.partitions` | `8` | Number of partitions when reshuffling data. The default is 200, which is excessive for a ~700 MB local dataset |
| `spark.sql.adaptive.enabled` | `true` | Enables adaptive query execution: Spark adjusts its execution plan at runtime based on actual data sizes |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Automatically merges small partitions after a shuffle to avoid unnecessary overhead |
| `spark.jars.packages` | mongo-spark-connector + postgresql | Auto-downloads the JDBC connectors for MongoDB and PostgreSQL, needed for cluster mode |

### Loading data

The notebook supports two data loading modes, selectable in the final cell:

#### Local mode

Reads the JSON files directly from `data/filtered_trimmed/`. No credentials or network connection required.

```python
cargar_vistas_local()
```

#### Cluster mode (demonstrates the distributed architecture)

Reads data directly from the three cloud services. Requires the `_READ` credentials configured in `.env`.

```python
cargar_vistas_clusters()
```

| Spark view | Cloud source |
|---|---|
| `business` | MongoDB Atlas |
| `tips` | MongoDB Atlas |
| `checkins` | MongoDB Atlas |
| `reviews` | Aiven PostgreSQL (JDBC) |
| `users` | CockroachDB (JDBC) |

Generated charts are saved automatically to the `graficos/` folder as PNG files (`q01_*.png` … `q10_*.png`).

---

### Query descriptions

#### Q01 — Market opportunity index by city
Computes a composite score per city by combining average rating, percentage of businesses still open, and total review volume. **Business value:** pinpoints which cities offer the most potential for opening or investing in a business, prioritizing markets with proven demand, viable businesses, and strong customer perception.

#### Q02 — Time-of-day segments and check-in traffic
Groups check-ins into four day segments (morning, afternoon, evening, late night) and identifies which businesses draw the most visits in each segment, by city. **Business value:** informs decisions around operating hours, staffing, and promotions based on when traffic actually flows in each market.

#### Q03 — Top-rated categories with meaningful review volume
Identifies business categories with the highest average rating, requiring at least 1,000 reviews for the result to be statistically meaningful. **Business value:** guides niche selection by showing which business types consistently achieve high customer satisfaction at scale.

#### Q04 — Price range vs. average rating by category
Crosses the price range attribute (`RestaurantsPriceRange2`) with average rating, segmented by category. **Business value:** reveals whether customers feel they're getting value at each price point and which categories offer the best quality-to-price ratio — useful for positioning and pricing strategy.

#### Q05 — Market leader ranking by category (Top 3)
Uses `DENSE_RANK` to surface the three top-performing businesses within each category, ordered by review volume and rating. **Business value:** maps who the benchmark players are in each niche, giving businesses a clear competitive reference for understanding who they're up against.

#### Q06 — Most influential users by review impact
Builds an influence index by weighting total reviews written, diversity of businesses reviewed, and total useful votes received. **Business value:** identifies the reviewers whose opinions carry the most real-world reach on the platform — useful for influencer marketing strategies or for weighting reviews in recommendation systems.

#### Q07 — Pandemic impact and recovery by state (2018–2021)
Calculates review volume by year and state, then measures year-over-year percentage change using the `LAG` window function. **Business value:** quantifies how hard COVID-19 hit Yelp activity in each state and lets you compare which markets bounced back fastest — relevant for post-pandemic expansion or reopening decisions.

#### Q08 — High-traffic businesses with low satisfaction
Combines check-in and review data to flag businesses that attract a lot of foot traffic (top 10% in their state) but carry a low rating (bottom 25%). **Business value:** these businesses are either an opportunity or a warning sign — they manage to pull customers in but fail to satisfy them. If the operational issues are fixed, they have the audience to become high-performers.

#### Q09 — Quality trends and monthly performance gap
Computes each business's monthly average rating and compares it against its own 3-month rolling average using a sliding window. **Business value:** catches sharp quality drops in specific businesses that may signal management changes, service issues, or one-off events. It works as an early-warning indicator for quality management.

#### Q10 — Business segmentation by clustering (K-Means)
Applies K-Means with 4 clusters over `stars`, `review_count`, and `total_checkins`, normalized with `StandardScaler`. **Business value:** groups businesses into natural segments based on their popularity and quality profile, without predefined labels. This enables platforms like Yelp to tailor the experience by business type or design differentiated strategies for each segment.

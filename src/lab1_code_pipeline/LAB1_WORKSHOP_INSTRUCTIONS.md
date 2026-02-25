# Lab 1 — Python ETL Pipelines: Polars, APIs & Object-Oriented Design

## Overview

In the previous session you built `refined_transformation_indicators.py` — a procedural pandas pipeline that extracts invoice and contract data from MySQL, computes KPIs, and loads them back into the database.

In this lab you will work through **four new scripts** that extend that foundation:

| # | Script | What you will learn |
|---|--------|---------------------|
| 1 | `refined_transformation_indicators_polars.py` | Rewrite a pandas pipeline in **Polars** (eager mode) |
| 2 | `refined_transformation_indicators_polars_v2.py` | Leverage **lazy evaluation**, reusable expressions & parallel execution |
| 3 | `meteo_pipeline.py` | Consume a **REST API**, store data in MySQL & build a new KPI |
| 4 | `refined_transformation_indicators_oop.py` | Refactor into **abstract base classes** for reusable, extensible pipelines |

All scripts include **execution time** and **peak RAM** measurements so you can compare approaches.

---

## Prerequisites

- Python 3.10+
- Packages: `pip install polars pandas numpy sqlalchemy pymysql pyyaml requests`
- MySQL access with the credentials file at `C:/Users/<you>/Documents/creds/creds_dmbi.yml`
- The tables `inv_invoice_ft`, `con_contract_dim`, `con_client_type_dim`, `inv_doc_type_dim` and `gen_zipcode_dim` already loaded in the database
- You have already reviewed and run `refined_transformation_indicators.py`

---

## Background: What is Polars?

**Polars** is a DataFrame library for Python (and Rust) designed as a modern, high-performance alternative to pandas. While pandas has been the standard tool for data manipulation in Python since 2008, it was designed for single-threaded, in-memory workloads. Polars was built from scratch to overcome these limitations.

### Why Polars exists

Pandas runs on a single CPU core and is constrained by Python's Global Interpreter Lock (GIL). As datasets grow, pandas can become slow and memory-hungry. Polars is written in **Rust** (a compiled, memory-safe language) and exposed to Python via bindings. This means the heavy computation happens outside Python, bypassing the GIL entirely.

### Key concepts

| Concept | Explanation |
|---------|-------------|
| **Eager execution** | Like pandas — each operation runs immediately and returns a result. Simple to understand, but every step materialises the full DataFrame in memory. |
| **Lazy execution** | Polars builds a **query plan** (a logical graph of operations) without executing anything. When you call `.collect()`, the engine **optimises** the plan (reordering operations, pushing filters down, eliminating unused columns) and then runs it. This is similar to how SQL databases optimise queries before executing them. |
| **Expressions** | Instead of operating on rows one at a time, Polars uses composable **expressions** like `pl.col("price") * pl.col("quantity")`. These are compiled to vectorised Rust code and can run in parallel. |
| **LazyFrame vs DataFrame** | A `DataFrame` holds data in memory (eager). A `LazyFrame` holds a query plan (lazy). You convert between them with `.lazy()` and `.collect()`. |
| **Parallel execution** | Polars automatically splits work across all available CPU cores. When you put multiple expressions in a single `.with_columns()`, they execute in parallel — no extra code needed. |
| **Strict typing** | Every column in Polars has a fixed type (Int64, Float64, Utf8, etc.). Unlike pandas, there is no "object" catch-all dtype. This forces cleaner data and enables faster processing. |
| **No index** | Polars does not use row indices. All operations are column-based. This avoids common pandas pitfalls like misaligned indices after joins. |

### Polars vs pandas — when to use which

| Scenario | Recommended |
|----------|-------------|
| Quick exploration in a notebook with < 100K rows | pandas |
| Production pipeline with millions of rows | Polars |
| Need lazy optimisation (filter pushdown, projection pruning) | Polars |
| Ecosystem compatibility (scikit-learn, matplotlib, etc.) | pandas (Polars can convert via `.to_pandas()`) |
| Team is already fluent in pandas | Start with pandas, migrate hot paths to Polars |

### connectorx — how Polars reads databases

When you call `pl.read_database_uri()`, Polars uses **connectorx** under the hood — a fast, Rust-based database connector. It reads directly into Apache Arrow memory (Polars' native format), skipping the overhead of Python objects. This is why database reads can be significantly faster than `pd.read_sql()`.

---

## Part 1 — Polars Eager Pipeline (≈ 20 min)

**Script:** `refined_transformation_indicators_polars.py`

### 1.1 Read the code

Open the script and compare it side-by-side with the pandas version. Identify the Polars equivalent for each pandas operation:

| Pandas | Polars |
|--------|--------|
| `pd.read_sql(query, engine)` | `pl.read_database_uri(query, uri=...)` |
| `df.drop_duplicates()` | `df.unique()` |
| `np.select(conditions, choices)` | `pl.when(...).then(...).otherwise(...)` |
| `pd.to_numeric(col, errors='coerce')` | `pl.col(...).cast(pl.Int64, strict=False)` |
| `df.merge(..., how='left')` | `df.join(..., how='left')` |
| `df.groupby(...).sum()` | `df.group_by(...).agg(pl.col(...).sum())` |
| `pd.concat([...])` | `pl.concat([...])` |

### 1.2 Run and compare

```bash
python refined_transformation_indicators.py
python refined_transformation_indicators_polars.py
```

**Questions to answer:**
1. Which pipeline was faster? By how much?
2. Which used less RAM?
3. Why did we need `.cast(pl.Float64)` on the `count()` KPI before concatenating?

### 1.3 Theory questions

- **Q1.1:** In Polars, `df.unique()` replaces pandas' `df.drop_duplicates()`. What parameter would you use in Polars to deduplicate based on a *subset* of columns? *(Hint: check the Polars docs for `unique()`)*
- **Q1.2:** The function `read_from_database_pl` uses `engine.url.render_as_string(hide_password=False)`. Why is `hide_password=False` necessary? What happens if you omit it?
- **Q1.3:** Polars uses **Apache Arrow** as its in-memory format, while pandas uses **NumPy arrays**. Name two advantages of Arrow over NumPy for tabular data.
- **Q1.4:** In the `classify_invoices_pl` function, what would happen if a value in `total_import_euros` is `null`? Which branch of the `when/then/otherwise` would it fall into?
- **Q1.5:** Why does Polars use `pl.col("contract_id").cast(pl.Int64, strict=False)` instead of just `.cast(pl.Int64)`? What does `strict=False` do with values that cannot be converted?

### 1.4 Exercises

- **Exercise 1.A:** Add a third KPI: *Average invoice amount by client type*. Use `pl.col("total_import_euros").mean()` in a `group_by`.
- **Exercise 1.B:** The `write_to_database_pl` function converts to pandas before writing. Why can't Polars write directly to MySQL? What alternatives exist?
- **Exercise 1.C:** Add a `.filter()` to the pipeline that removes rows where `total_import_euros <= 0` before computing the KPIs. Run both the original and filtered versions and compare the KPI values.
- **Exercise 1.D:** Modify the script to also print the number of rows and columns of each DataFrame after extraction (invoices and contracts). Use Polars' `.shape` property.

---

## Part 2 — Polars Lazy Pipeline (≈ 25 min)

**Script:** `refined_transformation_indicators_polars_v2.py`

### 2.1 Understand the key concepts

Read the docstring at the top of the file. It explains four Polars features:

1. **Lazy Evaluation** — `read_database_uri(...).lazy()` returns a `LazyFrame`. Nothing executes until `.collect()`.
2. **Expression API** — `CATEGORY_EXPR` and `CAST_CONTRACT_ID` are defined once as standalone expression objects.
3. **Parallel Execution** — Multiple columns in one `.with_columns()` run in parallel across CPU cores.
4. **Method Chaining** — Each operation produces a new `LazyFrame` without mutating the original.

### 2.2 Trace the execution

Answer these questions by reading the code:

1. At which line does Polars **actually start computing**? *(Hint: look for `.collect()`)*
2. What is the difference between `pl.DataFrame` and `pl.LazyFrame`?
3. The code defines `CATEGORY_EXPR` as a module-level variable. What is the advantage of this compared to defining the classification inside `main()`?
4. In the `invoices_lf.with_columns(CAST_CONTRACT_ID, CATEGORY_EXPR)` call, how many passes over the data does Polars need? How many would pandas need?

### 2.3 Run and compare

```bash
python refined_transformation_indicators_polars.py
python refined_transformation_indicators_polars_v2.py
```

**Questions to answer:**
1. Is the lazy version faster than the eager one? Why or why not for this dataset size?
2. Look at the output preview. Does the schema match what you expected?

### 2.4 Theory questions

- **Q2.1:** Explain in your own words what **predicate pushdown** means. Give an example of how the Polars optimizer could use it in this pipeline.
- **Q2.2:** What is **projection pushdown**? If the KPI only needs `total_import_euros` and `category`, which columns could the optimizer skip loading?
- **Q2.3:** The v2 script defines `CATEGORY_EXPR` and `CAST_CONTRACT_ID` as module-level variables. Could you do the same in pandas? Why or why not?
- **Q2.4:** If you call `.collect()` on a LazyFrame and then call `.lazy()` again on the result, do you lose the optimization benefits? Explain.
- **Q2.5:** The code calls `.collect()` twice (once per KPI LazyFrame). Would it be better to concatenate the two LazyFrames first and call `.collect()` once? What are the trade-offs?

### 2.5 Exercises

- **Exercise 2.A:** Add a `.filter()` step to the lazy pipeline that excludes invoices where `total_import_euros` is null *before* the join. Does this change the result? Does it change performance?
- **Exercise 2.B:** Create a new reusable expression (like `CATEGORY_EXPR`) that classifies `power_kw` from the contracts into "Low" (< 5), "Medium" (5–10), and "High" (> 10). Add a KPI that counts contracts per power category.
- **Exercise 2.C:** Use `lf.explain()` to print the optimised query plan of `kpi_category_lf` before calling `.collect()`. Paste the plan and identify which optimizations Polars applied.
- **Exercise 2.D:** Modify the pipeline to also compute *median* invoice amount by category (use `pl.col(...).median()`). Does this work in lazy mode? Research the difference between `median()` and `quantile(0.5)` in Polars.

---

## Background: What is an API?

### Definition

An **API** (Application Programming Interface) is a set of rules that allows one piece of software to communicate with another. In the context of data engineering, we typically work with **Web APIs** (also called **REST APIs**) — services accessible over the internet via HTTP, the same protocol your browser uses.

Think of an API as a **waiter in a restaurant**: you (the client) send a request from the menu (the API documentation), the kitchen (the server) prepares the data, and the waiter brings back the response.

### How a REST API works

```
   YOUR SCRIPT                    INTERNET                   API SERVER
  ┌───────────┐    HTTP Request    ┌─────────┐    Process    ┌──────────┐
  │  Python   │ ──────────────────>│  HTTP   │ ────────────> │  Server  │
  │  requests │ <──────────────────│  Layer  │ <──────────── │  (data)  │
  └───────────┘    HTTP Response   └─────────┘    Result     └──────────┘
```

1. Your script sends an **HTTP request** to a URL (called an **endpoint**).
2. The server processes the request and returns an **HTTP response** containing data (usually in **JSON** format).
3. Your script parses the JSON and converts it into a DataFrame.

### Key concepts

| Concept | Explanation |
|---------|-------------|
| **Endpoint** | The URL you call, e.g. `https://api.open-meteo.com/v1/forecast`. It identifies *what resource* you want. |
| **HTTP Methods** | `GET` = read data, `POST` = send data, `PUT` = update, `DELETE` = remove. For data pipelines, `GET` is the most common. |
| **Query Parameters** | Key-value pairs appended to the URL after `?`, e.g. `?latitude=40.42&longitude=-3.70`. They customise what data the API returns. |
| **JSON** | JavaScript Object Notation — a lightweight text format for structured data. It uses `{}` for objects and `[]` for arrays. Python's `dict` and `list` map directly to JSON. |
| **HTTP Status Codes** | `200` = success, `400` = bad request, `401` = unauthorized, `404` = not found, `429` = rate limited, `500` = server error. Always check the status code before parsing the response. |
| **Rate Limiting** | Most APIs limit how many requests you can make per minute/hour. Exceeding the limit returns a `429` error. Production pipelines need retry logic and backoff strategies. |
| **Authentication** | Some APIs require an **API key** or **OAuth token** in the request headers. Open-Meteo is free and requires no authentication, but most commercial APIs do. |

### The `requests` library

Python's `requests` library is the standard tool for calling APIs:

```python
import requests

response = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={"latitude": 40.42, "longitude": -3.70, "hourly": "temperature_2m"},
    timeout=30
)

response.raise_for_status()   # Raises an exception if status ≠ 200
data = response.json()         # Parses JSON into a Python dict
```

### Why APIs matter for data engineering

In real-world pipelines, data rarely comes only from databases. APIs let you integrate:
- **Weather data** (Open-Meteo, OpenWeatherMap)
- **Financial data** (stock prices, exchange rates)
- **Social media metrics** (Twitter/X, LinkedIn)
- **Internal microservices** (user profiles, product catalogs)
- **Government open data** (census, geographic boundaries)

The pattern is always the same: **request → parse → transform → load**.

### Open-Meteo — the API used in this lab

[Open-Meteo](https://open-meteo.com/) provides free weather forecast and historical data. Key features:
- No API key required for non-commercial use
- Returns up to 7 days of hourly forecasts
- Supports temperature, humidity, wind, precipitation, and many more variables
- Data is returned in JSON with two arrays: `time` (timestamps) and the requested variable (values)

---

## Part 3 — API Integration: Meteo Pipeline (≈ 25 min)

**Script:** `meteo_pipeline.py`

### 3.1 Understand the architecture

This pipeline introduces a new data source: the **Open-Meteo REST API**. Read the code and identify the six steps:

1. Load credentials and create the database engine
2. Query `gen_zipcode_dim` for the average latitude/longitude of Madrid and Barcelona
3. Call `https://api.open-meteo.com/v1/forecast` for each city
4. Store the hourly forecast in the `meteo_eae` table (truncate + replace)
5. Compute the average forecast temperature per city as a KPI
6. Write the KPI to `gen_kpi_ft`

### 3.2 Explore the API

Before running the script, open this URL in your browser:

```
https://api.open-meteo.com/v1/forecast?latitude=40.42&longitude=-3.70&hourly=temperature_2m
```

**Questions to answer:**
1. What structure does the JSON response have?
2. How many hours of forecast does it return?
3. What timezone is the data in?

### 3.3 Theory questions

- **Q3.1:** What HTTP method does `requests.get()` use? When would you use `requests.post()` instead?
- **Q3.2:** The code uses `response.raise_for_status()`. What happens if you remove this line and the API returns a 500 error? How would it affect the pipeline?
- **Q3.3:** Explain what a **timeout** parameter does in `requests.get(..., timeout=30)`. What would happen to the pipeline if the API is slow and we didn't set a timeout?
- **Q3.4:** The function `get_city_coordinates` builds a SQL query using f-strings with city names. Why is this approach potentially dangerous? What is **SQL injection** and how could you prevent it? *(Hint: parameterised queries)*
- **Q3.5:** The pipeline uses `if_exists="replace"` to store data in `meteo_eae`. How does this differ from `if_exists="append"`? In what scenario would `append` be more appropriate?
- **Q3.6:** What would happen if the Open-Meteo API changes its JSON response structure (e.g. renames `"hourly"` to `"data"`)? How would you make the pipeline more robust against such changes?

### 3.4 Run the script

```bash
python meteo_pipeline.py
```

Verify the results:
- Check the `meteo_eae` table in MySQL. How many rows were inserted?
- Check `gen_kpi_ft`. Do you see the two new temperature KPIs?

### 3.5 Exercises

- **Exercise 3.A:** Add a third city (e.g. Valencia or Bilbao) to the `CITIES` list and re-run. Confirm the new KPI appears.
- **Exercise 3.B:** The API supports additional variables. Modify `fetch_forecast` to also request `relative_humidity_2m`. Store it in a new column and create a KPI: *Average forecast humidity for {city}*.
- **Exercise 3.C:** The current code calls the API sequentially (one city at a time). As a bonus, research how you could call both cities in parallel using `concurrent.futures.ThreadPoolExecutor`.
- **Exercise 3.D:** Add error handling to `fetch_forecast` so that if one city fails (e.g. the API is down), the pipeline continues with the remaining cities and prints a warning instead of crashing.
- **Exercise 3.E:** Add a new KPI: *Max forecast temperature for {city}* and *Min forecast temperature for {city}*. You should end up with 6 KPIs (avg, max, min × 2 cities).

---

## Part 4 — Object-Oriented Pipeline (≈ 30 min)

**Script:** `refined_transformation_indicators_oop.py`

### 4.1 Study the class hierarchy

The code is organised into reusable abstract classes and concrete implementations:

```
DatabaseManager              — DB I/O (read, write, load_sql, load_credentials)
│
BaseTransformer (ABC)        — abstract: override transform(**dataframes)
│   └── InvoiceTransformer   — concrete: classify, cast, merge
│
BaseKPI (ABC)                — abstract: override build(df) → [kpi_name, kpi_value]
│   ├── AmountByCategoryKPI  — concrete: KPI 1
│   └── CountByDocTypeKPI    — concrete: KPI 2
│
BasePipeline (ABC)           — abstract: extract / get_transformer / get_kpis
    └── InvoiceKPIPipeline   — concrete: wires SQL files + transformer + KPIs
```

### 4.2 Identify the design patterns

**Questions to answer:**
1. **Template Method Pattern:** Which class defines `run()` and what methods does it call? Why don't subclasses need to override `run()`?
2. **Dependency Injection:** Where is `DatabaseManager` created? Why is it passed *into* the pipeline rather than created *inside* it?
3. **Open/Closed Principle:** If you need to add a new KPI, which classes do you modify? Which classes remain untouched?
4. **Single Responsibility:** What is the single responsibility of `BaseKPI`? Of `BaseTransformer`?

### 4.3 Run and compare

```bash
python refined_transformation_indicators.py
python refined_transformation_indicators_oop.py
```

The output and performance should be almost identical. The value is in **maintainability and extensibility**, not speed.

### 4.4 Theory questions

- **Q4.1:** What is an **Abstract Base Class (ABC)** in Python? What happens if you try to instantiate `BaseKPI()` directly without implementing `build()`?
- **Q4.2:** Explain the **Template Method Pattern** as used in `BasePipeline.run()`. Why is `run()` not abstract while `extract()`, `get_transformer()`, and `get_kpis()` are?
- **Q4.3:** What is **Dependency Injection**? In this code, what is being "injected" into `InvoiceKPIPipeline` and why is this better than creating a `DatabaseManager` inside the pipeline?
- **Q4.4:** The `InvoiceTransformer.transform()` method uses `**kwargs` with keyword-only arguments (`*, invoices, contracts`). What does the `*` mean? What error would you get if you called `transform(invoices_df, contracts_df)` without keyword arguments?
- **Q4.5:** If two developers need to add new KPIs to the same pipeline simultaneously, how does the OOP design reduce merge conflicts compared to the procedural approach?
- **Q4.6:** The `DatabaseManager` class uses `@staticmethod` for `load_sql()` and `load_credentials()`. Why are these static methods instead of regular instance methods? Could they be standalone functions instead?

### 4.5 Exercises

- **Exercise 4.A — New KPI (easy):** Create a class `AvgAmountByClientTypeKPI(BaseKPI)` that computes the average invoice amount per client type. Register it in `InvoiceKPIPipeline.get_kpis()` and re-run.

- **Exercise 4.B — New Pipeline (intermediate):** Using the existing abstract classes, create a `MeteoPipeline(BasePipeline)` that:
  - `extract()` → calls the Open-Meteo API (reuse the logic from `meteo_pipeline.py`)
  - `get_transformer()` → returns a transformer that just passes the data through
  - `get_kpis()` → returns a `AvgTemperatureKPI` that computes average temp per city
  - Wire it up in `__main__` alongside the `InvoiceKPIPipeline` sharing the same `DatabaseManager`

- **Exercise 4.C — Reflection (discussion):** The OOP version has more lines of code than the procedural version. In what scenarios is this trade-off worth it? When would you prefer the simpler procedural approach?

- **Exercise 4.D — Unit testing:** Write a simple test for `AmountByCategoryKPI.build()` using a small hardcoded DataFrame (3-5 rows). Verify that the output has the expected columns and the KPI values are correct. *(Hint: you don't need a database connection for this — just create a DataFrame with the required columns)*

- **Exercise 4.E — Inheritance challenge:** Create a `FilteredInvoiceKPIPipeline` that extends `InvoiceKPIPipeline` but overrides `get_transformer()` to return a transformer that filters out invoices older than 2020 before merging. Run both pipelines and compare the KPI results.

---

## Summary & Comparison

After completing the lab, fill in this table with your results:

| Script | Approach | Time (s) | RAM (MB) | Lines of Code |
|--------|----------|----------|----------|---------------|
| `refined_transformation_indicators.py` | Pandas (procedural) | | | |
| `refined_transformation_indicators_polars.py` | Polars (eager) | | | |
| `refined_transformation_indicators_polars_v2.py` | Polars (lazy) | | | |
| `refined_transformation_indicators_oop.py` | Pandas (OOP) | | | |
| `meteo_pipeline.py` | Pandas + REST API | | | |

**Discussion questions for the class:**
1. When would you choose Polars over pandas?
2. What are the benefits of lazy evaluation even for small datasets?
3. How does the OOP approach help when a team of 5 developers works on the same ETL project?
4. What are the challenges of integrating external APIs into an ETL pipeline (rate limits, error handling, data freshness)?

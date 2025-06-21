# 🌦️ Weather Data Pipeline with dbt, Snowflake & CI/CD

This project is a modern data engineering pipeline that fetches real-time weather data, loads it into **Snowflake**, transforms it using **dbt**, and automatically generates **interactive documentation** via **GitHub Actions**.

---

## 🚀 Tech Stack

- **Python** — Fetches and formats weather data  
- **Docker** — Containerizes the ingestion job  
- **Snowflake** — Cloud data warehouse for storage & transformations  
- **dbt** — Transforms raw data and builds lineage  
- **GitHub Actions** — Automates docs generation & deployment to GitHub Pages

---

## 📂 Project Structure

```

data\_engineer/
├── python\_jobs/
│   ├── weather.py        # Fetches weather data and loads to Snowflake
│   └── .env              # Contains Snowflake credentials and API keys
│
├── sql\_examples/         # dbt project
│   ├── models/
│   │   ├── my\_first\_dbt\_model.sql
│   │   ├── weather\_data\_model.sql
│   │   └── ...
│   ├── dbt\_project.yml
│   └── ...
│
├── Dockerfile            # Containerizes the weather ingestion job
├── .github/
│   └── workflows/
│       └── dbt-docs.yml  # CI/CD to auto-generate and deploy docs

````

---

## 🛠️ Setup Instructions

### ✅ 1. Clone the Repo

```bash
git clone https://github.com/ali-raza01/Portfolio.git
cd Portfolio/data_engineer
````

### 🐳 2. Build and Run Docker

```bash
docker build -t weather-pipeline .
docker run --env-file python_jobs/.env weather-pipeline
```

> `.env` contains sensitive credentials (Snowflake, OpenWeather API). This file is excluded from Git tracking.

---

## 🧠 dbt: Data Transformations

```bash
# Navigate into dbt project
cd sql_examples

# Run dbt transformations
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## 🔁 CI/CD with GitHub Actions

Every push to `main`:

* Runs `dbt docs generate`
* Publishes docs to [GitHub Pages](https://ali-raza01.github.io/Portfolio)

Make sure:

* You've set **repository secret**: `DBT` (value is your `profiles.yml`)
* GitHub Actions have **read/write permissions**:
  Settings → Actions → General → Workflow permissions → ✅ *Read and write*

---

## 🌐 Live Documentation

📘 [View dbt Docs](https://ali-raza01.github.io/Portfolio)

---

## 📌 Author

**Ali Raza**
MSc Data Analytics | Portfolio: [ali-raza01.github.io/Portfolio](https://ali-raza01.github.io/Portfolio)

---

## 📝 License

This project is licensed under the MIT License.

```

# 🧱 Data Engineering Portfolio

Welcome to my personal data engineering portfolio!  
This project demonstrates real-world data engineering patterns using:

- ❄️ **Snowflake** for scalable cloud data warehousing
- 🐍 **Python** with Snowpark for ELT and data integration
- 📦 **dbt** for SQL-based data modeling and testing
- 🐳 **Docker** for reproducibility
- ⚙️ **CI/CD** pipelines for automation and quality checks

---

## 🔧 Tech Stack

| Category         | Tools/Tech                      |
|------------------|----------------------------------|
| Data Platform    | Snowflake                        |
| Orchestration    | dbt, Snowflake Tasks             |
| Programming      | Python (Snowpark, requests)      |
| Automation       | GitHub Actions                   |
| Dev Environment  | Docker, DevContainers (VSCode)   |
| Linting & Tests  | SQLFluff, PyTest                 |

---

## 📁 Project Structure

```bash
.
├── python_jobs/         # Snowpark Python scripts and loaders
├── sql_examples/        # SQL scripts outside of dbt
├── dbt_project/         # dbt models, seeds, snapshots
├── snowflake_cicd/      # Streams, Tasks, CI scripts
├── docker/              # Dockerfiles and Docker Compose setup
├── .devcontainer/       # Optional VSCode remote setup
├── README.md
└── LICENSE
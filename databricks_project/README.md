Here's a complete and clean `README.md` file tailored for your **Twitter Sentiment Analysis Pipeline on Databricks**, using the **same stack** and structure you've built so far:

---

```markdown
# 🐦 Twitter Sentiment Analysis with Databricks

A data engineering pipeline that collects tweets via Twitter API, performs sentiment analysis, and stores the results in Delta format for visualization on Databricks.

---

## 📌 Project Structure

```

databricks\_project/
│
├── twitter\_pipeline/
│   ├── ingest/                 # Collect tweets using Twitter API
│   ├── preprocess/             # Clean and analyze sentiments
│   ├── aggregate/              # Summarize sentiment counts
│   ├── utils/                  # Helper scripts (API handling, upload functions)
│   ├── notebooks/              # Local notebooks (uploaded to Databricks)
│   └── .env                    # Contains Twitter API bearer token (not committed)

```

---

## 🔧 Technology Stack

- **Databricks**
- **Apache Spark**
- **Delta Lake**
- **Twitter API v2**
- **PySpark**
- **TextBlob (sentiment analysis)**
- **DBFS** for file storage
- **Databricks SQL** for visualization

---

## ⚙️ Workflow Overview

### 1. Collect Tweets (`ingest_tweets.py`)
- Connects to the Twitter API v2
- Saves recent tweets matching a keyword (e.g., `databricks`, `spark`)
- Stores them locally as JSON and uploads to `dbfs:/FileStore/twitter/raw/`

### 2. Sentiment Processing (`sentiment_analysis.py`)
- Reads raw tweet data from DBFS
- Applies `TextBlob` to calculate sentiment polarity
- Categorizes into: **positive**, **neutral**, **negative**

### 3. Aggregation (`aggregate_sentiment.py`)
- Aggregates tweet counts by `date` and `sentiment_label`
- Outputs a summary DataFrame
- Saves the summary to Delta format at `dbfs:/user/hive/warehouse/tweets_sentiment_gold`

### 4. Visualization (Databricks SQL)
- Uses `display(df_summary)` for visual exploration
- Visualizations include **line charts** of sentiment over time

---

## 🔐 Secrets Management

To keep your Twitter credentials safe:
1. Create a `.env` file (not committed to Git) in `twitter_pipeline/`:
```

TWITTER\_BEARER\_TOKEN=your\_token\_here

````
2. Load it with `python-dotenv`:
```python
from dotenv import load_dotenv
load_dotenv(dotenv_path="../.env")
````

---

## 🚀 Example Query

```sql
SELECT * FROM default.tweets_sentiment_gold
WHERE date = '2025-06-22'
ORDER BY sentiment_label
```

---

## 📊 Example Visualization

You can visualize results by:

* Clicking the 📊 icon under `display(df_summary)`
* Choosing **Line Chart**
* Setting:

  * X-axis: `date`
  * Y-axis: `tweet_count`
  * Series: `sentiment_label`

---

## 📁 DBFS Paths

| Purpose          | Path                                         |
| ---------------- | -------------------------------------------- |
| Raw tweets       | `/FileStore/twitter/raw/`                    |
| Delta Table      | `/user/hive/warehouse/tweets_sentiment_gold` |
| Final Table Name | `default.tweets_sentiment_gold`              |

---

## 🧠 Future Enhancements

* Automate ingestion with Databricks Jobs
* Add dashboard alerts for sentiment spikes
* Add location-based analysis or trending hashtags
* Incorporate real-time streaming (Kafka or Webhooks)

---

## 👨‍💻 Author

Shahzad Ali Raza
\[MSc Data Analytics Candidate]

---

## 📄 License

This project is for educational purposes.
Ensure compliance with Twitter's [Developer Policy](https://developer.twitter.com/en/developer-terms/policy) when using the API.

```

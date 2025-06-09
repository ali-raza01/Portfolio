🔍 This project delves into the use of Airbnb listings in Amsterdam across various data sets such as property information, host reviews, and neighborhood boundaries. The aim is to identify trends for listings, geographic trends, and predict prices on Airbnb using machine learning. The project showcases the following skills:

- Data Cleaning and Preprocessing

- Exploratory Data Analysis (EDA)

- Geospatial Visualization with GeoJSON

- Feature Engineering for machine learning

- Regression Modeling (ML)

- Deployment-ready data pipeline structure

🎯 Project Goal
To build a data-driven model that can:

Predict Airbnb prices based on listing attributes.

Provide actionable insights about listing distribution and activity by neighbourhood.

📌 Objectives
Data Cleaning: Handle missing values, correct types, and merge datasets.

EDA: Analyze price distributions, review activity, and availability trends.

Geospatial Analysis: Map average prices and reviews per neighbourhood.

Feature Engineering: Create new variables like review frequency, host activity level, etc.

ML Modeling: Train regression models (e.g., Random Forest, XGBoost) to predict listing price.

Evaluation: Use RMSE and R² to evaluate performance.

Visualization: Plot trends, correlations, and geospatial insights.



## 📊 Exploratory Data Analysis (EDA)

To understand the structure and dynamics of Airbnb listings in Amsterdam, I performed an exploratory analysis using seaborn and matplotlib.

### 💸 **Price Distribution**

* The majority of listings are priced under **€200**, with a strong concentration around **€100–150**.
* The distribution is **right-skewed**, indicating a few luxury listings with very high prices.
* Recommendation: consider **log-transforming price** to reduce skewness in modeling.

---

### 🏠 **Room Type Breakdown**

* **Private rooms** dominate the listings, followed by **entire homes/apartments**.
* **Shared rooms** and **hotel rooms** represent a tiny fraction of the market.
* This suggests Amsterdam’s Airbnb market is largely peer-to-peer rather than commercial.

---

### 📆 **Availability by Room Type**

* **Entire homes** show a wide range of availability — from seasonal to full-year listings.
* **Private rooms** tend to be available part-time, likely due to shared living arrangements.
* Outliers with `365` availability may indicate full-time short-term rental businesses.

---

### 🔗 **Feature Correlation**

* `number_of_reviews` and `reviews_per_month` are **strongly correlated**, indicating review activity scales with longevity or popularity.
* Surprisingly, `price` shows **low linear correlation** with most variables, reinforcing the need for **nonlinear modeling approaches** like Random Forests or XGBoost.

---

### 🏙️ **Top 10 Most Expensive Neighbourhoods**

* Neighbourhoods such as **Weesperbuurt en Plantage**, **Museumkwartier**, and **Grachtengordel-Zuid** have the **highest average prices**.
* These are typically **central, upscale, or tourist-heavy** locations.

---

### 🧑‍💼 **Listings per Host**

* Most hosts have only **1 listing**, but there’s a long tail of **multi-property hosts**, some with over **20 listings**.
* Indicates a mix of casual users and professional hosts or property managers.


## 🌍 Geospatial Insights

To understand how Airbnb listing characteristics vary across Amsterdam’s neighbourhoods, I performed geospatial analysis using GeoJSON boundaries and interactive maps built with `folium`.

### 🏷️ **Average Price per Neighbourhood**

* Central neighbourhoods like **Centrum-West**, **Grachtengordel-Zuid**, and **Museumkwartier** exhibit **the highest average prices**, highlighting their premium tourist appeal.
* In contrast, peripheral areas such as **Noord** and **Bijlmer-Oost** tend to have **lower-priced listings**, suggesting lower market demand or local-oriented rental activity.
* This spatial distribution is essential for:

  * Pricing strategies by hosts
  * Identifying high-return zones for investment

> 📌 View the interactive map: [`avg_price_map.html`](./outputs/figures/avg_price_map.html)


### 💬 **Review Density by Neighbourhood**

* Areas like **De Pijp**, **Centrum-Oost**, and **Oud-West** show **the highest review counts**, indicating high guest engagement and frequent bookings.
* Conversely, less-reviewed regions might either represent **underperforming zones** or **emerging markets**.
* This insight supports:

  * Demand forecasting
  * Host performance benchmarking
  * Strategic expansion for property managers

> 📌 View the interactive map: [`review_count_map.html`](./outputs/figures/review_count_map.html)


These maps visually reinforce how location influences both pricing and customer interaction, providing actionable insights for hosts, investors, and urban planners.


## 🤖 Price Prediction with Machine Learning

To estimate the nightly price of Airbnb listings in Amsterdam, I built supervised learning models using a combination of numerical and categorical features.

### 🔧 Features Used

* Room Type (One-hot encoded: Private, Shared, Hotel room)
* Availability (in days per year)
* Minimum nights required
* Host listing count
* Number and frequency of reviews
* Days since last review

> 💡 Price was log-transformed (`log(price + 1)`) to reduce right skew and improve regression performance.


### 🧠 Models Trained

| Model         | RMSE (log-scale) | R² Score  |
| ------------- | ---------------- | --------- |
| Random Forest | 0.379            | 0.394     |
| XGBoost       | **0.374**        | **0.411** |

* The XGBoost model outperformed Random Forest, achieving lower RMSE and better R².
* While R² is modest, this is expected due to the complexity and noise in price-setting on Airbnb.


### 📊 Feature Importance (Random Forest)

![Feature Importance](./outputs/figures/feature_importance.png)

* **Private room** indicator was the most important feature, followed by `availability_365`, `days_since_review`, and `reviews_per_month`.
* Listing types and review activity are major predictors of price.


### 🚀 Future Improvements

* Incorporate **location-based features** (e.g., latitude/longitude clustering or neighbourhood encodings).
* Use **textual data** (e.g., listing names or descriptions) via NLP techniques.
* Add **calendar/seasonality features** (e.g., holiday or weekend flags).


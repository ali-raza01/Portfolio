# src/data_preprocessing.py

import pandas as pd
import os

RAW_DIR = "../data/raw"
PROCESSED_DIR = "../data/processed"

def load_data():
    listings = pd.read_csv(os.path.join(RAW_DIR, "listings.csv"))
    reviews = pd.read_csv(os.path.join(RAW_DIR, "reviews.csv"))
    neighbourhoods = pd.read_csv(os.path.join(RAW_DIR, "neighbourhoods.csv"))
    return listings, reviews, neighbourhoods

def clean_listings(listings):
    # Drop empty 'neighbourhood_group'
    listings.drop(columns=["neighbourhood_group"], inplace=True, errors='ignore')

    # Convert 'last_review' to datetime
    listings["last_review"] = pd.to_datetime(listings["last_review"], errors="coerce")

    # Fill missing 'reviews_per_month' with 0 where 'number_of_reviews' is 0
    listings["reviews_per_month"] = listings["reviews_per_month"].fillna(0)

    # Drop rows with missing 'price' or 'neighbourhood'
    listings = listings.dropna(subset=["price", "neighbourhood"])

    return listings

def clean_reviews(reviews):
    reviews["date"] = pd.to_datetime(reviews["date"], errors="coerce")
    return reviews

def merge_data(listings, neighbourhoods):
    merged = listings.merge(neighbourhoods, on="neighbourhood", how="left")
    return merged

def save_clean_data(listings, reviews, merged):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    listings.to_csv(os.path.join(PROCESSED_DIR, "clean_listings.csv"), index=False)
    reviews.to_csv(os.path.join(PROCESSED_DIR, "clean_reviews.csv"), index=False)
    merged.to_csv(os.path.join(PROCESSED_DIR, "merged_data.csv"), index=False)

def run_pipeline():
    print("🔄 Loading data...")
    listings, reviews, neighbourhoods = load_data()
    
    print("🧼 Cleaning data...")
    listings_clean = clean_listings(listings)
    reviews_clean = clean_reviews(reviews)
    
    print("🔗 Merging datasets...")
    merged = merge_data(listings_clean, neighbourhoods)

    print("💾 Saving cleaned data...")
    save_clean_data(listings_clean, reviews_clean, merged)
    
    print("✅ Data pipeline completed!")

if __name__ == "__main__":
    run_pipeline()

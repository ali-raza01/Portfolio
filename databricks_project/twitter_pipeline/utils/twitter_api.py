import requests

def search_recent_tweets(query, bearer_token, max_results=20):
    headers = {"Authorization": f"Bearer {bearer_token}"}
    endpoint = "https://api.twitter.com/2/tweets/search/recent"

    params = {
        "query": query,
        "max_results": max_results,
        "tweet.fields": "created_at,author_id,lang"
    }

    response = requests.get(endpoint, headers=headers, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Twitter API error: {response.status_code} - {response.text}")
    
    return response.json().get("data", [])

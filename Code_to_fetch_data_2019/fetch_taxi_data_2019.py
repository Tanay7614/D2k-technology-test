import os
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

BASE_URL = "https://www.nyc.gov"
DATA_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DATA_DIR = "data"
YEAR = 2019

os.makedirs(DATA_DIR, exist_ok=True)

def get_2019_parquet_links():
    """Scrape the webpage to get Parquet file links for 2019."""
    print(f"Fetching data links from {DATA_PAGE}...")
    response = requests.get(DATA_PAGE)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a", href=True)
    
    parquet_links = []
    for link in links:
        href = link["href"]
        if f"{YEAR}" in href and href.endswith(".parquet"):
            parquet_links.append(BASE_URL + href if href.startswith("/") else href)
    
    print(f"Found {len(parquet_links)} Parquet files for {YEAR}.")
    return parquet_links

@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=2, max=60))
def download_parquet_file(url, filepath):
    """Download a Parquet file with retries."""
    print(f"Attempting to download {url}...")
    response = requests.get(url, stream=True, timeout=10)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        for chunk in tqdm(response.iter_content(chunk_size=1024), desc=f"Saving {os.path.basename(filepath)}"):
            f.write(chunk)
    print(f"Successfully downloaded {url} to {filepath}")

def download_2019_data():
    """Download all 2019 Parquet files."""
    links = get_2019_parquet_links()
    for link in links:
        filename = os.path.basename(link)
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            try:
                download_parquet_file(link, filepath)
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {filename}: {e}")
        else:
            print(f"{filename} already exists. Skipping.")

if __name__ == "__main__":
    download_2019_data()

import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
from urllib.parse import urljoin
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Base URL templates
HOME_URL = "https://vlxx.bz/"
PAGE_URL = "https://vlxx.bz/new/{index}/"

# Google Sheets config
SHEET_ID = '1kMGN_Yfzz5MJdOzNIePBNcdCN4fRvrkCFz2uO3x40uE'  # Replace with your Google Sheet ID
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'  # Written by GitHub Actions from secret

# Lock for thread-safe operations
sheets_lock = threading.Lock()

# Queue for pages to scrape
page_queue = queue.Queue()

# Flag to stop scraping
stop_scraping = False

# All data collected
all_video_data = []

# Function to scrape a single page
def scrape_page(page_num):
    global stop_scraping
    if stop_scraping:
        return []

    url = HOME_URL if page_num == 1 else PAGE_URL.format(index=page_num)
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_num}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.find_all('div', class_='video-item')

    if not items and page_num > 1000:
        print(f"No data on page {page_num}, likely reached end.")
        stop_scraping = True
        return []
    elif not items:
        print(f"No data on page {page_num}, but continuing.")
        return []

    video_data = []
    for item in items:
        video_id = item.get('id', '').replace('video-', '') if item.get('id') else 'N/A'
        a_tag = item.find('a')
        title = a_tag.get('title', 'N/A') if a_tag else 'N/A'
        link = urljoin("https://vlxx.bz", a_tag.get('href', 'N/A')) if a_tag else 'N/A'
        
        img_tag = item.find('img', class_='video-image')
        thumbnail = img_tag.get('data-original', img_tag.get('src', 'N/A')) if img_tag else 'N/A'
        thumbnail = urljoin("https://vlxx.bz", thumbnail) if thumbnail != 'N/A' and not thumbnail.startswith('http') else thumbnail
        
        ribbon_div = item.find('div', class_='ribbon')
        ribbon = ribbon_div.text.strip() if ribbon_div else 'N/A'
        
        data = {
            'page': page_num,
            'id': video_id,
            'title': title,
            'link': link,
            'thumbnail': thumbnail,
            'ribbon': ribbon
        }
        video_data.append(data)
        all_video_data.append(data)

    return video_data

# Worker threads
def worker():
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
        except queue.Empty:
            break
        
        data = scrape_page(page_num)
        if data:
            print(f"Scraped page {page_num} ({len(data)} videos)")
        
        page_queue.task_done()
        time.sleep(1)

# Function to update Google Sheets
def update_google_sheets():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        
        df = pd.DataFrame(all_video_data)
        values = [df.columns.values.tolist()] + df.values.tolist()
        
        with sheets_lock:
            sheet.clear()
            sheet.update('A1', values)
        print("Updated Google Sheets successfully!")
    except Exception as e:
        print(f"Error updating Sheets: {e}")

# Main function
def main(num_threads=5, max_pages=10000):
    global stop_scraping

    for page_num in range(1, max_pages + 1):
        page_queue.put(page_num)

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

    if all_video_data:
        update_google_sheets()
    else:
        print("No data to update.")

if __name__ == "__main__":
    main(num_threads=5, max_pages=10000)
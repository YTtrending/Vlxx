import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
from urllib.parse import urljoin
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os

# Base URL templates
HOME_URL = "https://vlxx.bz/"
PAGE_URL = "https://vlxx.bz/new/{index}/"

# Google Sheets config
SHEET_ID = 'YOUR_SHEET_ID_HERE'  # Replace with your Google Sheet ID
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'

# Temp CSV for debugging (local to GitHub Actions)
TEMP_CSV = 'temp_videos.csv'

# Locks
sheets_lock = threading.Lock()

# Queue and flag
page_queue = queue.Queue()
stop_scraping = False
all_video_data = []

# Scrape a single page
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

    if not items:  # Stop immediately if no items
        print(f"No data on page {page_num}, stopping.")
        stop_scraping = True
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

# Worker thread
def worker():
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
        except queue.Empty:
            break
        
        start_time = time.time()
        data = scrape_page(page_num)
        elapsed = time.time() - start_time
        if data:
            print(f"Scraped page {page_num} ({len(data)} videos) in {elapsed:.2f}s")
        
        page_queue.task_done()
        time.sleep(0.5)  # Reduced delay for speed

# Update Google Sheets
def update_google_sheets():
    if not os.path.exists(CREDENTIALS_FILE):
        print("Error: credentials.json not found!")
        return

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        
        if all_video_data:
            df = pd.DataFrame(all_video_data)
            df = df.sort_values(by=['page', 'id'])  # Sort for readability
            values = [df.columns.values.tolist()] + df.values.tolist()
            
            with sheets_lock:
                sheet.clear()
                sheet.update('A1', values)  # Write from A1
            print(f"Updated Google Sheets: {len(all_video_data)} rows across {df['page'].max()} pages")
            
            # Save temp CSV for debugging
            df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
            print(f"Saved temp CSV: {TEMP_CSV}")
        else:
            print("No data to update.")
    except Exception as e:
        print(f"Error updating Sheets: {e}")

# Main function
def main(num_threads=10, max_pages=200):  # Optimized for ~100 pages
    global stop_scraping
    start_total = time.time()

    for page_num in range(1, max_pages + 1):
        page_queue.put(page_num)

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

    elapsed_total = time.time() - start_total
    print(f"Total scrape time: {elapsed_total:.2f}s for up to {max_pages} pages")

    if all_video_data:
        update_google_sheets()

if __name__ == "__main__":
    main()

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
import json

# Base URL templates
HOME_URL = "https://vlxx.bz/"
PAGE_URL = "https://vlxx.bz/new/{index}/"
# Google Sheets config
SHEET_ID = '1kMGN_Yfzz5MJdOzNIePBNcdCN4fRvrkCFz2uO3x40uE'  # Your Sheet ID
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'
# Output files
TEMP_CSV = 'temp_videos.csv'
DATA_TXT = 'data.txt'
# Locks
sheets_lock = threading.Lock()
# Queue and flag
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
all_video_data = []
# Expected columns
EXPECTED_COLUMNS = [
    'page', 'id', 'title', 'link', 'thumbnail', 'ribbon',
    'detailed_scraped', 'iframe_src', 'likes', 'dislikes', 'rating',
    'views', 'video_code', 'video_link', 'description', 'actress',
    'categories', 'last_updated'
]

# Scrape pagination page
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
    if not items:
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
            'ribbon': ribbon,
            'detailed_scraped': None,  # Track if detailed scraped
            'last_updated': 0  # Initial last updated
        }
        video_data.append(data)
        all_video_data.append(data)
    return video_data

# Worker for pagination
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
        time.sleep(0.5)

# Scrape detail page
def scrape_detail(detail_link):
    try:
        response = requests.get(detail_link, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching detail {detail_link}: {e}")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    video_div = soup.find('div', id='video')
    if not video_div:
        return None
    detail_data = {}
    detail_data['video_id'] = video_div.get('data-id', 'N/A')
    # Get iframe src
    iframe = soup.find('iframe', src=True)
    detail_data['iframe_src'] = iframe.get('src', 'N/A') if iframe else 'N/A'
    # Stats
    stats_div = soup.find('div', class_='video-stats')
    if stats_div:
        detail_data['likes'] = stats_div.find('span', class_='likes').text.strip() if stats_div.find('span', class_='likes') else 'N/A'
        detail_data['dislikes'] = stats_div.find('span', class_='dislikes').text.strip() if stats_div.find('span', class_='dislikes') else 'N/A'
        detail_data['rating'] = stats_div.find('span', class_='rating').text.strip() if stats_div.find('span', class_='rating') else 'N/A'
        detail_data['views'] = stats_div.find('span', class_='views').text.strip() if stats_div.find('span', class_='views') else 'N/A'
    # Info
    info_div = soup.find('div', class_='video-info')
    if info_div:
        detail_data['video_code'] = info_div.find('span', class_='video-code').text.strip() if info_div.find('span', class_='video-code') else 'N/A'
        detail_data['video_link'] = info_div.find('span', class_='video-link').text.strip() if info_div.find('span', class_='video-link') else 'N/A'
    # Description
    desc_div = soup.find('div', class_='video-description')
    detail_data['description'] = desc_div.text.strip()[:500] + '...' if desc_div and len(desc_div.text.strip()) > 500 else (desc_div.text.strip() if desc_div else 'N/A')
    # Tags
    actress_div = soup.find('div', class_='actress-tag')
    detail_data['actress'] = actress_div.find('a').get('title', 'N/A') if actress_div and actress_div.find('a') else 'N/A'
    category_div = soup.find('div', class_='category-tag')
    categories = [a.get('title', '') for a in category_div.find_all('a')] if category_div else []
    detail_data['categories'] = '; '.join(categories) if categories else 'N/A'
    # Update timestamp
    detail_data['last_updated'] = time.time()
    return detail_data

# Worker for details
def detail_worker():
    while True:
        try:
            detail_link = detail_queue.get_nowait()
        except queue.Empty:
            break
       
        start_time = time.time()
        detail_data = scrape_detail(detail_link)
        elapsed = time.time() - start_time
        if detail_data:
            print(f"Scraped detail for {detail_link} in {elapsed:.2f}s")
            # Merge into all_video_data by id
            for video in all_video_data:
                if video['id'] == detail_data['video_id']:
                    video.update(detail_data)
                    video['detailed_scraped'] = 'true'
                    break
        else:
            print(f"No detail data for {detail_link}")
       
        detail_queue.task_done()
        time.sleep(1)  # Delay to avoid rate limiting

# Get pending details from Sheet
def get_pending_details():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        records = sheet.get_all_records()
        current_time = time.time()
        WEEK_SECONDS = 7 * 86400
        pending = []
        for row in records:
            detailed_scraped = row.get('detailed_scraped', '')
            last_updated = row.get('last_updated', 0)
            try:
                last_updated = float(last_updated)
            except ValueError:
                last_updated = 0
            if detailed_scraped != 'true' or (detailed_scraped == 'true' and (current_time - last_updated) > WEEK_SECONDS):
                pending.append(row['link'])
        print(f"Found {len(pending)} pending details to scrape")
        return pending
    except Exception as e:
        print(f"Error getting pending: {e}")
        return []

# Load existing data from data.txt for updating page numbers
def load_existing_data():
    existing_data = []
    if os.path.exists(DATA_TXT):
        try:
            with open(DATA_TXT, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except Exception as e:
            print(f"Error loading {DATA_TXT}: {e}")
    return existing_data

# Save data.txt as JSON, sorted by page and id
def save_data_txt():
    try:
        # Load existing data
        existing_data = load_existing_data()
        existing_dict = {item['link']: item for item in existing_data}
        
        # Update page numbers for existing links, add new entries
        for video in all_video_data:
            if video['link'] in existing_dict:
                existing_video = existing_dict[video['link']]
                existing_video['page'] = video['page']  # Update page number
                # Update other fields if detailed scraped
                if video.get('detailed_scraped') == 'true':
                    existing_video.update({k: v for k, v in video.items() if k in EXPECTED_COLUMNS})
            else:
                existing_dict[video['link']] = video
        
        # Convert back to list and sort by page and id
        sorted_data = sorted(existing_dict.values(), key=lambda x: (x.get('page', 0), x.get('id', '')))
        
        # Create DataFrame to handle columns
        df = pd.DataFrame(sorted_data)
        # Add missing columns with 'N/A'
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = 'N/A'
        # Remove extra columns
        df = df[EXPECTED_COLUMNS]
        sorted_data = df.to_dict('records')
        
        with open(DATA_TXT, 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
        print(f"Saved {DATA_TXT}: {len(sorted_data)} records")
    except Exception as e:
        print(f"Error saving {DATA_TXT}: {e}")

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
            # Load existing data
            existing_data = load_existing_data()
            existing_dict = {item['link']: item for item in existing_data}
            
            # Update page numbers for existing links, add new entries
            for video in all_video_data:
                if video['link'] in existing_dict:
                    existing_video = existing_dict[video['link']]
                    existing_video['page'] = video['page']  # Update page number
                    # Update other fields if detailed scraped
                    if video.get('detailed_scraped') == 'true':
                        existing_video.update({k: v for k, v in video.items() if k in EXPECTED_COLUMNS})
                else:
                    existing_dict[video['link']] = video
            
            # Convert to DataFrame and sort by page and id
            df = pd.DataFrame(list(existing_dict.values()))
            df = df.sort_values(by=['page', 'id'])
            
            # Add missing columns with 'N/A'
            for col in EXPECTED_COLUMNS:
                if col not in df.columns:
                    df[col] = 'N/A'
            # Remove extra columns
            df = df[EXPECTED_COLUMNS]
            
            # Save to temp CSV
            df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
            print(f"Saved temp CSV: {TEMP_CSV}")
           
            # Update Google Sheets
            values = [df.columns.values.tolist()] + df.values.tolist()
            with sheets_lock:
                sheet.clear()
                sheet.update('A1', values)
            print(f"Updated Google Sheets: {len(df)} rows across {df['page'].max()} pages")
        else:
            print("No data to update.")
    except Exception as e:
        print(f"Error updating Sheets: {e}")

# Main function
def main(num_threads=10, max_pages=200, detail_threads=15):
    global stop_scraping
    start_total = time.time()
    
    # Step 1: Scrape pagination (new videos)
    for page_num in range(1, max_pages + 1):
        page_queue.put(page_num)
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
   
    for t in threads:
        t.join()
    
    # Step 2: Get pending details (new + old chưa scrape or old >7 days)
    pending_links = get_pending_details()
    for link in pending_links:
        detail_queue.put(link)
    
    # Step 3: Scrape details song song
    detail_threads_list = []
    for _ in range(detail_threads):
        t = threading.Thread(target=detail_worker)
        t.start()
        detail_threads_list.append(t)
   
    for t in detail_threads_list:
        t.join()
    
    elapsed_total = time.time() - start_total
    print(f"Total time: {elapsed_total:.2f}s")
    
    if all_video_data:
        save_data_txt()
        update_google_sheets()

if __name__ == "__main__":
    main()

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

# Config
HOME_URL = "https://vlxx.bz/"
PAGE_URL = "https://vlxx.bz/new/{index}/"
SHEET_ID = '1kMGN_Yfzz5MJdOzNIePBNcdCN4fRvrkCFz2uO3x40uE'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'
TEMP_CSV = 'temp_videos.csv'
DATA_TXT = 'data.txt'
sheets_lock = threading.Lock()
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
all_video_data = []
EXPECTED_COLUMNS = [
    'page', 'id', 'title', 'link', 'thumbnail', 'ribbon',
    'detailed_scraped', 'likes', 'dislikes', 'rating',
    'views', 'video_code', 'video_link', 'description', 'actress',
    'categories', 'last_updated'
]
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Referer': 'https://vlxx.bz/',
    'Accept-Language': 'en-US,en;q=0.9'
}

# Scrape pagination page
def scrape_page(page_num):
    global stop_scraping
    if stop_scraping:
        return []
    url = HOME_URL if page_num == 1 else PAGE_URL.format(index=page_num)
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.find_all('div', class_='video-item')
    if not items:
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
            'detailed_scraped': None,
            'last_updated': 0
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
        scrape_page(page_num)
        page_queue.task_done()
        time.sleep(0.5)

# Scrape detail page
def scrape_detail(detail_link, retries=5):
    for attempt in range(retries):
        try:
            response = requests.get(detail_link, headers=HEADERS, timeout=20)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException:
            if attempt + 1 == retries:
                return None
            time.sleep(3)
    soup = BeautifulSoup(response.text, 'html.parser')
    video_div = soup.find('div', id='video')
    if not video_div:
        return None
    detail_data = {}
    detail_data['video_id'] = video_div.get('data-id', 'N/A')
    stats_div = soup.find('div', class_='video-stats')
    if stats_div:
        detail_data['likes'] = stats_div.find('span', class_='likes').text.strip() if stats_div.find('span', class_='likes') else 'N/A'
        detail_data['dislikes'] = stats_div.find('span', class_='dislikes').text.strip() if stats_div.find('span', class_='dislikes') else 'N/A'
        detail_data['rating'] = stats_div.find('span', class_='rating').text.strip() if stats_div.find('span', class_='rating') else 'N/A'
        detail_data['views'] = stats_div.find('span', class_='views').text.strip() if stats_div.find('span', class_='views') else 'N/A'
    info_div = soup.find('div', class_='video-info')
    if info_div:
        detail_data['video_code'] = info_div.find('span', class_='video-code').text.strip() if info_div.find('span', class_='video-code') else 'N/A'
        detail_data['video_link'] = info_div.find('span', class_='video-link').text.strip() if info_div.find('span', class_='video-link') else 'N/A'
    desc_div = soup.find('div', class_='video-description')
    detail_data['description'] = desc_div.text.strip()[:500] + '...' if desc_div and len(desc_div.text.strip()) > 500 else (desc_div.text.strip() if desc_div else 'N/A')
    actress_div = soup.find('div', class_='actress-tag')
    detail_data['actress'] = actress_div.find('a').get('title', 'N/A') if actress_div and actress_div.find('a') else 'N/A'
    category_div = soup.find('div', class_='category-tag')
    categories = [a.get('title', '') for a in category_div.find_all('a')] if category_div else []
    detail_data['categories'] = '; '.join(categories) if categories else 'N/A'
    detail_data['last_updated'] = time.time()
    return detail_data

# Worker for details
def detail_worker():
    while True:
        try:
            detail_link = detail_queue.get_nowait()
        except queue.Empty:
            break
        detail_data = scrape_detail(detail_link)
        if detail_data:
            for video in all_video_data:
                if video['id'] == detail_data['video_id']:
                    video.update(detail_data)
                    video['detailed_scraped'] = 'true'
                    break
        detail_queue.task_done()
        time.sleep(1)

# Get pending details from Sheet
def get_pending_details(existing_links):
    pending = []
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        records = sheet.get_all_records()
        current_time = time.time()
        WEEK_SECONDS = 7 * 86400
        for row in records:
            detailed_scraped = row.get('detailed_scraped', '')
            last_updated = row.get('last_updated', 0)
            try:
                last_updated = float(last_updated)
            except ValueError:
                last_updated = 0
            if detailed_scraped != 'true' or (detailed_scraped == 'true' and (current_time - last_updated) > WEEK_SECONDS):
                if row['link'] not in existing_links:
                    pending.append(row['link'])
    except Exception as e:
        print(f"Error getting Google Sheet: {e}")
    return pending

# Load existing data from data.txt
def load_existing_data():
    if os.path.exists(DATA_TXT):
        try:
            with open(DATA_TXT, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading {DATA_TXT}: {e}")
    return []

# Save data.txt
def save_data_txt():
    try:
        existing_data = load_existing_data()
        existing_dict = {item['link']: item for item in existing_data}
        for video in all_video_data:
            if video['link'] in existing_dict:
                existing_video = existing_dict[video['link']]
                existing_video['page'] = video['page']
                if video.get('detailed_scraped') == 'true':
                    existing_video.update({k: v for k, v in video.items() if k in EXPECTED_COLUMNS})
            else:
                existing_dict[video['link']] = video
        sorted_data = sorted(existing_dict.values(), key=lambda x: int(x.get('id', '0')) if x.get('id', '0').isdigit() else 0, reverse=True)
        df = pd.DataFrame(sorted_data)
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = 'N/A'
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
            existing_data = load_existing_data()
            existing_dict = {item['link']: item for item in existing_data}
            for video in all_video_data:
                if video['link'] in existing_dict:
                    existing_video = existing_dict[video['link']]
                    existing_video['page'] = video['page']
                    if video.get('detailed_scraped') == 'true':
                        existing_video.update({k: v for k, v in video.items() if k in EXPECTED_COLUMNS})
                else:
                    existing_dict[video['link']] = video
            df = pd.DataFrame(list(existing_dict.values()))
            df = df.sort_values(by='id', key=lambda x: x.astype(int), ascending=False)
            for col in EXPECTED_COLUMNS:
                if col not in df.columns:
                    df[col] = 'N/A'
            df = df[EXPECTED_COLUMNS]
            df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
            print(f"Saved temp CSV: {TEMP_CSV}")
            values = [df.columns.values.tolist()] + df.values.tolist()
            with sheets_lock:
                sheet.clear()
                sheet.update('A1', values)
            print(f"Updated Google Sheets: {len(df)} rows")
    except Exception as e:
        print(f"Error updating Sheets: {e}")

# Main function
def main(num_threads=10, detail_threads=15):
    global stop_scraping
    start_total = time.time()
    
    # Step 1: Scrape pagination until no items
    page_num = 1
    while not stop_scraping:
        page_queue.put(page_num)
        page_num += 1
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    
    # Step 2: Add all new video links to detail queue
    existing_links = set()
    for video in all_video_data:
        if video['link'] != 'N/A' and video['detailed_scraped'] is None:
            if video['link'] not in existing_links:
                detail_queue.put(video['link'])
                existing_links.add(video['link'])
    
    # Step 3: Get pending details from Google Sheet
    pending_links = get_pending_details(existing_links)
    for link in pending_links:
        if link not in existing_links:
            detail_queue.put(link)
            existing_links.add(link)
    
    # Step 4: Scrape details
    if not detail_queue.empty():
        print(f"Scraping {detail_queue.qsize()} detail links")
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

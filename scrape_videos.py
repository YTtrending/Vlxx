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

# ... (giữ config cũ: HOME_URL, PAGE_URL, SHEET_ID, etc.)

all_video_data = []  # Sẽ chứa {list_data + detail_data}

def scrape_detail_page(url):
    """Scrape chi tiết từ link video"""
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 ...'}, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        detail = {}
        video_div = soup.find('div', id='video')
        if video_div:
            detail['video_id'] = video_div.get('data-id', 'N/A')
            detail['sv'] = video_div.get('data-sv', 'N/A')
        
        iframe = soup.find('iframe')
        detail['embed_url'] = iframe.get('src', 'N/A') if iframe else 'N/A'
        
        # Servers (simple count or list)
        servers = [li.get('id', '').replace('server', '') for li in soup.find_all('li', class_='video-server')]
        detail['servers'] = ','.join(servers) if servers else 'N/A'
        
        # Stats
        stats_div = soup.find('div', class_='video-stats')
        if stats_div:
            detail['likes'] = stats_div.find('span', class_='likes').text.strip() if stats_div.find('span', class_='likes') else 'N/A'
            detail['dislikes'] = stats_div.find('span', class_='dislikes').text.strip() if stats_div.find('span', class_='dislikes') else 'N/A'
            detail['rating'] = stats_div.find('span', class_='rating').text.strip() if stats_div.find('span', class_='rating') else 'N/A'
            detail['views'] = stats_div.find('span', class_='views').text.strip() if stats_div.find('span', class_='views') else 'N/A'
        
        # Video info
        info_div = soup.find('div', class_='video-info')
        if info_div:
            detail['video_code'] = info_div.find('span', class_='video-code').text.strip() if info_div.find('span', class_='video-code') else 'N/A'
            detail['video_link'] = info_div.find('span', class_='video-link').text.strip() if info_div.find('span', class_='video-link') else 'N/A'
        
        # Description
        desc_div = soup.find('div', class_='video-description')
        detail['description'] = desc_div.text.strip()[:1000] if desc_div else 'N/A'  # Cắt ngắn
        
        # Tags
        actress_div = soup.find('div', class_='actress-tag')
        detail['actress'] = ', '.join([a.get('title', '') for a in actress_div.find_all('a')]) if actress_div else 'N/A'
        
        category_div = soup.find('div', class_='category-tag')
        detail['categories'] = ', '.join([a.get('title', '') for a in category_div.find_all('a')]) if category_div else 'N/A'
        
        return detail
    except Exception as e:
        print(f"Error scraping detail {url}: {e}")
        return {}

def scrape_page(page_num):  # Giữ nguyên, nhưng return list_data với 'link' để scrape sau
    # ... (code scrape list cũ)
    # Trong loop for item:
    data = { ... }  # Như cũ
    data['detailed_scraped'] = False  # Mặc định chưa scrape chi tiết
    video_data.append(data)
    all_video_data.append(data)
    return video_data

# Trong main, sau scrape list:
def update_details_from_sheets():
    """Lấy existing IDs từ Sheets để chỉ scrape mới"""
    try:
        creds = ...  # Auth như cũ
        sheet = client.open_by_key(SHEET_ID).sheet1
        existing = sheet.get_all_records()
        existing_ids = {row.get('id', '') for row in existing if row.get('detailed_scraped', False) == 'True'}
        return existing_ids
    except:
        return set()

# Sau scrape list, enqueue chi tiết cho new IDs
existing_ids = update_details_from_sheets()
new_links = [d['link'] for d in all_video_data if d['id'] not in existing_ids]

detail_queue = queue.Queue()
for link in new_links:
    detail_queue.put(link)

def detail_worker():
    while True:
        try:
            link = detail_queue.get_nowait()
            detail = scrape_detail_page(link)
            if detail:
                # Merge vào all_video_data dựa trên id
                for vid in all_video_data:
                    if vid['id'] == detail['video_id']:
                        vid.update(detail)
                        vid['detailed_scraped'] = True
                        break
            detail_queue.task_done()
            time.sleep(1)  # Delay cho chi tiết
        except queue.Empty:
            break

# Start detail threads (5 threads cho chi tiết)
detail_threads = [threading.Thread(target=detail_worker) for _ in range(5)]
for t in detail_threads: t.start()
for t in detail_threads: t.join()

# Sau đó save_data_txt và append/update Sheets (thêm rows mới hoặc update existing)
df = pd.DataFrame(all_video_data).sort_values('page')
# Append nếu new, hoặc update full
sheet.append_rows(df.values.tolist()) if new else ...  # Logic append

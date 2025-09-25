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

# Locks
sheets_lock = threading.Lock()

# Queues and flags
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
queueing_complete = False
all_video_data = []

# Load config from config.json
def load_config():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Lỗi khi đọc config.json: {e}")
        return {}

# Load existing data from data.txt
def load_existing_data(config):
    if os.path.exists(config['DATA_TXT']):
        try:
            with open(config['DATA_TXT'], 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            return existing_data
        except Exception:
            return []
    return []

# Scrape pagination page
def scrape_page(page_num, config):
    global stop_scraping
    if stop_scraping:
        return []

    url = f"{config['DOMAIN']}/" if page_num == 1 else f"{config['DOMAIN']}/new/{page_num}/"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
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
        link = urljoin(config['DOMAIN'], a_tag.get('href', 'N/A')) if a_tag else 'N/A'
        
        img_tag = item.find('img', class_='video-image')
        thumbnail = img_tag.get('data-original', img_tag.get('src', 'N/A')) if img_tag else 'N/A'
        thumbnail = urljoin(config['DOMAIN'], thumbnail) if thumbnail != 'N/A' and not thumbnail.startswith('http') else thumbnail
        
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
        with sheets_lock:
            if not any(v['id'] == video_id and v['link'] == link for v in all_video_data):
                all_video_data.append(data)

    return video_data

# Worker for pagination
def worker(config):
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
        except queue.Empty:
            break
        
        scrape_page(page_num, config)
        page_queue.task_done()
        time.sleep(0.5)

# Convert views to number
def convert_views(views_str):
    try:
        views_str = views_str.lower().replace(',', '')
        if 'k' in views_str:
            return int(float(views_str.replace('k', '')) * 1000)
        elif 'm' in views_str:
            return int(float(views_str.replace('m', '')) * 1000000)
        return int(views_str)
    except (ValueError, AttributeError):
        return 0

# Convert likes/dislikes to number
def convert_likes_dislikes(value):
    try:
        return int(value.replace('.', ''))
    except (ValueError, AttributeError):
        return 0

# Convert rating to number
def convert_rating(rating_str):
    try:
        return int(rating_str.replace('%', ''))
    except (ValueError, AttributeError):
        return 0

# Scrape detail page
def scrape_detail(detail_link):
    try:
        response = requests.get(detail_link, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    video_div = soup.find('div', id='video')
    if not video_div:
        return None

    detail_data = {}
    detail_data['video_id'] = video_div.get('data-id', 'N/A')

    stats_div = soup.find('div', class_='video-stats')
    if stats_div:
        detail_data['likes'] = convert_likes_dislikes(stats_div.find('span', class_='likes').text.strip() if stats_div.find('span', class_='likes') else '0')
        detail_data['dislikes'] = convert_likes_dislikes(stats_div.find('span', class_='dislikes').text.strip() if stats_div.find('span', class_='dislikes') else '0')
        detail_data['rating'] = convert_rating(stats_div.find('span', class_='rating').text.strip() if stats_div.find('span', class_='rating') else '0')
        detail_data['views'] = convert_views(stats_div.find('span', class_='views').text.strip() if stats_div.find('span', class_='views') else '0')

    info_div = soup.find('div', class_='video-info')
    if info_div:
        detail_data['video_code'] = info_div.find('span', class_='video-code').text.strip() if info_div.find('span', class_='video-code') else 'N/A'
        detail_data['video_link'] = info_div.find('span', class_='video-link').text.strip() if info_div.find('span', class_='video-link') else 'N/A'

    desc_div = soup.find('div', class_='video-description')
    detail_data['description'] = desc_div.text.strip()[:500] + '...' if desc_div and len(desc_div.text.strip()) > 500 else (desc_div.text.strip() if desc_div else 'N/A')

    actress_div = soup.find('div', class_='actress-tag')
    detail_data['actress'] = actress_div.find('a').get('title', 'N/A') if actress_div and actress_div.find('a') else 'N/A'

    return detail_data

# Worker for details
def detail_worker(config):
    while not (queueing_complete and detail_queue.empty()):
        try:
            detail_link = detail_queue.get(timeout=5)
            if detail_link is None:
                detail_queue.task_done()
                break

            detail_data = scrape_detail(detail_link)
            if detail_data:
                print(f"Đã scrape chi tiết cho {detail_link}")
                with sheets_lock:
                    for video in all_video_data:
                        if video['id'] == detail_data['video_id'] and video['link'] == detail_link:
                            video.update(detail_data)
                            break
            
            detail_queue.task_done()
            time.sleep(config['DETAIL_DELAY'])
        except queue.Empty:
            if queueing_complete:
                break
        except Exception:
            detail_queue.task_done()

# Get pending details from Sheet
def get_pending_details(config):
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['CREDENTIALS_FILE'], config['SCOPE'])
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config['SHEET_ID']).sheet1
        records = sheet.get_all_records()
        return [row['link'] for row in records if 'link' in row and row['link'] != 'N/A']
    except Exception:
        return []

# Save data.txt as JSON, sorted by page (asc) and id (desc)
def save_data_txt(config):
    try:
        df = pd.DataFrame(all_video_data)
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df = df.drop_duplicates(subset=['id', 'link'], keep='last')
        df = df.sort_values(by=['page', 'id'], ascending=[True, False])
        sorted_data = df.to_dict('records')
        with open(config['DATA_TXT'], 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# Update Google Sheets
def update_google_sheets(config):
    if not os.path.exists(config['CREDENTIALS_FILE']):
        return

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['CREDENTIALS_FILE'], config['SCOPE'])
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config['SHEET_ID']).sheet1
        
        if all_video_data:
            df = pd.DataFrame(all_video_data)
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            df = df.drop_duplicates(subset=['id', 'link'], keep='last')
            df = df.sort_values(by=['page', 'id'], ascending=[True, False])
            values = [df.columns.values.tolist()] + df.values.tolist()
            
            df.to_csv(config['TEMP_CSV'], index=False, encoding='utf-8')
            with sheets_lock:
                sheet.clear()
                sheet.update(values=values, range_name='A1')
    except Exception:
        pass

# Main function
def main():
    global stop_scraping, queueing_complete
    start_total = time.time()

    # Load config
    config = load_config()
    if not config:
        print("Không thể đọc config.json, thoát!")
        return

    # Step 1: Load existing data from data.txt
    all_video_data.extend(load_existing_data(config))

    # Step 2: Scrape pagination
    for page_num in range(1, config['MAX_PAGES'] + 1):
        page_queue.put(page_num)

    threads = []
    for _ in range(config['NUM_THREADS']):
        t = threading.Thread(target=worker, args=(config,))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

    # Step 3: Get pending details
    pending_links = get_pending_details(config)
    new_unscraped = [video['link'] for video in all_video_data if 'link' in video and video['link'] != 'N/A']
    pending_links.extend(new_unscraped)
    pending_links = list(set(pending_links))  # Remove duplicates

    # Step 4: Queue detail links
    for link in pending_links:
        detail_queue.put(link)

    queueing_complete = True

    # Step 5: Scrape details with multiple threads
    detail_threads_list = []
    for _ in range(min(config['DETAIL_THREADS'], len(pending_links))):
        t = threading.Thread(target=detail_worker, args=(config,))
        t.start()
        detail_threads_list.append(t)
    
    for t in detail_threads_list:
        t.join()

    while not detail_queue.empty():
        try:
            detail_queue.get_nowait()
            detail_queue.task_done()
        except queue.Empty:
            break

    # Summary
    total_pages = len(set(video['page'] for video in all_video_data if video['page'] != 'N/A'))
    total_videos = len(all_video_data)
    total_detailed = len([video for video in all_video_data if 'views' in video])
    elapsed_total = time.time() - start_total
    print(f"Tổng kết: {total_pages} trang, {total_videos} video, {total_detailed} video chi tiết, {elapsed_total:.2f}s")

    if all_video_data:
        save_data_txt(config)
        update_google_sheets(config)

if __name__ == "__main__":
    main()

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

# Configuration parameters
NUM_THREADS = 20  # Số luồng cho scrape pagination
DETAIL_THREADS = 20  # Số luồng cho scrape chi tiết
MAX_PAGES = 200  # Số trang tối đa để scrape
DETAIL_DELAY = 1.0  # Thời gian delay (giây) giữa các request chi tiết

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

# Queues and flags
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
queueing_complete = False  # Flag để báo hiệu queueing chi tiết hoàn tất
all_video_data = []

# Load existing data from data.txt
def load_existing_data():
    if os.path.exists(DATA_TXT):
        try:
            with open(DATA_TXT, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            print(f"Đọc được {len(existing_data)} bản ghi từ {DATA_TXT}")
            return existing_data
        except Exception as e:
            print(f"Lỗi khi đọc {DATA_TXT}: {e}")
            return []
    else:
        print(f"Không tìm thấy {DATA_TXT}, bắt đầu với dữ liệu rỗng")
        return []

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
        print(f"Lỗi khi lấy trang {page_num}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.find_all('div', class_='video-item')

    if not items:
        print(f"Không có dữ liệu trên trang {page_num}, dừng lại.")
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
            'detailed_scraped': None  # Track if detailed scraped
        }
        video_data.append(data)
        # Only append if ID doesn't exist in all_video_data
        with sheets_lock:
            if not any(v['id'] == video_id for v in all_video_data):
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
            print(f"Đã scrape trang {page_num} ({len(data)} video) trong {elapsed:.2f}s")
        
        page_queue.task_done()
        time.sleep(0.5)

# Scrape detail page
def scrape_detail(detail_link):
    print(f"Bắt đầu scrape chi tiết cho {detail_link}")
    try:
        response = requests.get(detail_link, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy chi tiết {detail_link}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    video_div = soup.find('div', id='video')
    if not video_div:
        print(f"Không tìm thấy div video cho {detail_link}")
        return None

    detail_data = {}
    detail_data['video_id'] = video_div.get('data-id', 'N/A')
    detail_data['data_sv'] = video_div.get('data-sv', 'N/A')

    # Servers
    servers = soup.find_all('li', class_='video-server')
    detail_data['server_count'] = len(servers)
    if servers:
        iframe = soup.find('iframe', src=True)
        detail_data['server1_embed'] = iframe.get('src') if iframe else 'N/A'
        detail_data['server2_embed'] = 'N/A'  # Placeholder, as server2 may require JS

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

    return detail_data

# Worker for details
def detail_worker():
    while not (queueing_complete and detail_queue.empty()):
        try:
            detail_link = detail_queue.get(timeout=5)  # Wait up to 5s for a link
            if detail_link is None:  # Sentinel value to exit
                print("Nhận được sentinel, worker thoát")
                detail_queue.task_done()
                break

            start_time = time.time()
            detail_data = scrape_detail(detail_link)
            elapsed = time.time() - start_time
            if detail_data:
                print(f"Đã scrape chi tiết cho {detail_link} trong {elapsed:.2f}s")
                # Merge into all_video_data by id
                with sheets_lock:
                    for video in all_video_data:
                        if video['id'] == detail_data['video_id']:
                            video.update(detail_data)
                            video['detailed_scraped'] = 'true'
                            break
            else:
                print(f"Không lấy được chi tiết cho {detail_link}")
            
            detail_queue.task_done()
            time.sleep(DETAIL_DELAY)  # Delay to avoid rate-limiting
        except queue.Empty:
            if queueing_complete:
                print("Hàng đợi chi tiết rỗng và queueing hoàn tất, worker thoát")
                break
            continue
        except Exception as e:
            print(f"Lỗi trong detail worker cho {detail_link}: {e}")
            detail_queue.task_done()

# Get pending details from Sheet
def get_pending_details():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        records = sheet.get_all_records()
        pending = [row['link'] for row in records if row.get('detailed_scraped', '') != 'true' and row.get('link', '') != 'N/A']
        print(f"Tìm thấy {len(pending)} link chi tiết cần scrape từ Google Sheets")
        return pending
    except Exception as e:
        print(f"Lỗi khi lấy link chi tiết từ Google Sheets: {e}")
        return []

# Save data.txt as JSON, sorted by page (asc) and id (desc)
def save_data_txt():
    try:
        df = pd.DataFrame(all_video_data)
        df['id'] = pd.to_numeric(df['id'], errors='coerce')  # Convert id to numeric for sorting
        df = df.sort_values(by=['page', 'id'], ascending=[True, False])  # Pages asc, IDs desc
        sorted_data = df.to_dict('records')
        with open(DATA_TXT, 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
        print(f"Lưu {DATA_TXT}: {len(sorted_data)} bản ghi")
    except Exception as e:
        print(f"Lỗi khi lưu {DATA_TXT}: {e}")

# Update Google Sheets
def update_google_sheets():
    if not os.path.exists(CREDENTIALS_FILE):
        print("Lỗi: Không tìm thấy credentials.json!")
        return

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        
        if all_video_data:
            df = pd.DataFrame(all_video_data)
            df['id'] = pd.to_numeric(df['id'], errors='coerce')  # Convert id to numeric for sorting
            df = df.sort_values(by=['page', 'id'], ascending=[True, False])  # Pages asc, IDs desc
            values = [df.columns.values.tolist()] + df.values.tolist()
            
            df.to_csv(TEMP_CSV, index=False, encoding='utf-8')
            print(f"Lưu CSV tạm: {TEMP_CSV}")
            
            with sheets_lock:
                sheet.clear()
                sheet.update(values=values, range_name='A1')
            print(f"Cập nhật Google Sheets: {len(all_video_data)} hàng trên {df['page'].max()} trang")
        else:
            print("Không có dữ liệu để cập nhật.")
    except Exception as e:
        print(f"Lỗi khi cập nhật Google Sheets: {e}")

# Main function
def main():
    global stop_scraping, queueing_complete
    start_total = time.time()

    # Step 1: Load existing data from data.txt
    existing_data = load_existing_data()
    with sheets_lock:
        all_video_data.extend(existing_data)
    print(f"Tổng số bản ghi sau khi đọc dữ liệu cũ: {len(all_video_data)}")

    # Step 2: Scrape pagination
    for page_num in range(1, MAX_PAGES + 1):
        page_queue.put(page_num)

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

    # Step 3: Get pending details (from Google Sheets + new unscraped videos)
    pending_links = get_pending_details()
    new_unscraped = [video['link'] for video in all_video_data if video['detailed_scraped'] != 'true' and video['link'] != 'N/A']
    pending_links.extend(new_unscraped)
    pending_links = list(set(pending_links))  # Remove duplicates
    print(f"Tổng số link chi tiết cần scrape: {len(pending_links)}")

    # Step 4: Queue detail links
    for link in pending_links:
        print(f"Đưa vào hàng đợi chi tiết: {link}")
        detail_queue.put(link)

    # Signal that queueing is complete
    queueing_complete = True
    print("Đã hoàn tất đưa link vào hàng đợi chi tiết, bắt đầu các worker chi tiết")

    # Step 5: Scrape details with multiple threads
    detail_threads_list = []
    for _ in range(min(DETAIL_THREADS, len(pending_links))):  # Avoid unnecessary threads
        t = threading.Thread(target=detail_worker)
        t.start()
        detail_threads_list.append(t)
    
    # Wait for all detail threads to finish
    for t in detail_threads_list:
        t.join()

    # Ensure all tasks are marked as done
    while not detail_queue.empty():
        try:
            detail_queue.get_nowait()
            detail_queue.task_done()
        except queue.Empty:
            break

    # Summary
    total_pages = len(set(video['page'] for video in all_video_data if video['page'] != 'N/A'))
    total_videos = len(all_video_data)
    total_detailed = len([video for video in all_video_data if video['detailed_scraped'] == 'true'])
    elapsed_total = time.time() - start_total
    print(f"Tổng kết:")
    print(f"  Tổng số trang đã scrape: {total_pages}")
    print(f"  Tổng số video thu thập: {total_videos}")
    print(f"  Tổng số video có chi tiết: {total_detailed}")
    print(f"  Tổng thời gian: {elapsed_total:.2f}s")

    if all_video_data:
        save_data_txt()
        update_google_sheets()

if __name__ == "__main__":
    main()

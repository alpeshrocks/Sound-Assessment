#!/usr/bin/env python3
import re, io, sys, pickle
from pathlib import Path
from collections import deque

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ---- CONFIG ----
ROOT           = Path(__file__).parent
CLIENT_SECRETS = ROOT / 'cred.json'
TOKEN_PICKLE   = ROOT / 'token.pickle'

DRIVE_ROOT_FOLDER_ID = '1T4lh501yhxxfhmzRMwLpBqKt5kpH4U2f'
SPREADSHEET_ID       = '1BPvhCQ8biYAj_x1UT3sYtgZCacxJLCPy0MJ7YPpBbzA'
SHEET_NAME           = 'Main Sheet'

TARGET_CLASSES       = ["SOS B37"]   # or [] for all classrooms

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

DEBUG = True
def dbg(msg):
    if DEBUG:
        print(msg)

# ---- REGEX ----
INDEX_RE = re.compile(r'_(\d{3})_123_Report\.txt$', re.IGNORECASE)

# ---- AUTH ----
def authenticate():
    creds = None
    if TOKEN_PICKLE.exists():
        creds = pickle.loads(TOKEN_PICKLE.read_bytes())
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS), SCOPES)
            creds = flow.run_local_server()
        TOKEN_PICKLE.write_bytes(pickle.dumps(creds))
    return creds

# ---- HELPERS ----
def _keyize(name: str) -> str:
    if not name:
        return ''
    s = name.replace('_', ' ')
    return re.sub(r'[^A-Za-z0-9]', '', s).upper()

def list_children(svc, parent_id):
    files, page_token = [], None
    while True:
        resp = svc.files().list(
            q=f"'{parent_id}' in parents and trashed=false",
            fields='nextPageToken, files(id,name,mimeType)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page_token
        ).execute()
        files.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break
    return files

def descend_to_folder(svc, parent_id, name):
    target_key = _keyize(name)
    for f in list_children(svc, parent_id):
        if f['mimeType'] == 'application/vnd.google-apps.folder' and _keyize(f['name']) == target_key:
            return f['id'], f['name']
    return None, None

def _list_named_child_folders(svc, parent_id, name):
    target_key = _keyize(name)
    matches = []
    for f in list_children(svc, parent_id):
        if f['mimeType'] == 'application/vnd.google-apps.folder' and _keyize(f['name']) == target_key:
            matches.append(f)
    return matches

def _folder_has_any_report(svc, folder_id, max_depth=6) -> bool:
    dq = deque([(folder_id, 0)])
    while dq:
        fid, d = dq.popleft()
        if d > max_depth:
            continue
        for ch in list_children(svc, fid):
            if ch['mimeType'] != 'application/vnd.google-apps.folder' and INDEX_RE.search(ch['name']):
                return True
            if ch['mimeType'] == 'application/vnd.google-apps.folder':
                dq.append((ch['id'], d + 1))
    return False

def choose_class_folder_with_content(svc, parent_id, class_name):
    candidates = _list_named_child_folders(svc, parent_id, class_name)
    if not candidates:
        return None
    for cand in candidates:
        if _folder_has_any_report(svc, cand['id']):
            return cand['id']
    return candidates[0]['id']

# ---- STRICT FILE MATCH ----
def is_target_report_for_index(name: str, idx: str) -> bool:
    """Return True only if file name ends with _<idx>_123_Report.txt"""
    m = INDEX_RE.search(name)
    return bool(m and m.group(1) == idx)

def list_files_recursive(svc, parent_id, max_depth=2):
    dq = deque([(parent_id, 0)])
    while dq:
        fid, d = dq.popleft()
        for ch in list_children(svc, fid):
            if ch['mimeType'] == 'application/vnd.google-apps.folder':
                if d < max_depth:
                    dq.append((ch['id'], d + 1))
            else:
                yield ch

def find_report_file_by_index(svc, folder_id, idx):
    """Find file matching ..._<idx>_123_Report.txt"""
    idx = f"{int(idx):03d}"
    matches = [f for f in list_files_recursive(svc, folder_id, max_depth=2)
               if is_target_report_for_index(f['name'], idx)]
    if not matches:
        return None, None
    matches.sort(key=lambda f: f['name'])
    chosen = matches[-1]
    return chosen['id'], chosen['name']

def download_file_text(svc, file_id):
    request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue().decode('utf-8', errors='ignore')

# ---- PARSING ----
def extract_laeq(text):
    """
    Extract the LAeq numeric value (5th token in a valid data line)
    """
    for line in text.splitlines():
        toks = line.split()
        if (
            len(toks) >= 5
            and re.match(r'\d{4}-\d{2}-\d{2}', toks[0])
            and re.match(r'\d{2}:\d{2}:\d{2}', toks[1])
            and re.match(r'\d{4}-\d{2}-\d{2}', toks[2])
            and re.match(r'\d{2}:\d{2}:\d{2}', toks[3])
        ):
            return toks[4]
    return None

# ---- SHEETS ----
def map_sheet_rows(svc):
    try:
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A4:A240"
        ).execute().get('values', [])
    except HttpError as e:
        print(f"[ERROR] Sheets API error: {e}")
        sys.exit(1)

    m = {}
    for idx, row in enumerate(resp, start=4):
        if row and row[0].strip():
            m[_keyize(row[0].strip())] = idx
    return m

# ---- COLUMN MAP ----
REMOTE_MAP = {
    'Remote3': {
        '000': ('Z',  'Left'),
        '001': ('AA', 'Center'),
        '002': ('AB', 'Right')
    },
    'Remote4': {
        '003': ('AC', 'Left'),
        '004': ('AD', 'Center'),
        '005': ('AE', 'Right')
    }
}

# ---- CORE ----
def process_one_classroom(drive_svc, sheets_updates, bname, b_id, class_name, row):
    c_id = choose_class_folder_with_content(drive_svc, b_id, class_name)
    if not c_id:
        dbg(f"  ✗ Classroom folder '{class_name}' not found under '{bname}'")
        return
    dbg(f"  • Processing classroom '{class_name}' (row {row})")

    for remote_name, slm_map in REMOTE_MAP.items():
        rid, _ = descend_to_folder(drive_svc, c_id, remote_name)
        if not rid:
            dbg(f"    ✗ Missing folder {remote_name}")
            continue

        for idx, (col, label) in slm_map.items():
            file_id, fname = find_report_file_by_index(drive_svc, rid, idx)
            if not file_id:
                dbg(f"      ✗ Index {idx} ({label}) not found as *_123_Report.txt")
                continue

            text = download_file_text(drive_svc, file_id)
            laeq = extract_laeq(text)
            if not laeq:
                dbg(f"      ✗ Could not parse LAeq from {fname}")
                continue

            dbg(f"      ✓ {fname} -> LAeq={laeq} -> {col}{row}")
            sheets_updates.append({'range': f"{SHEET_NAME}!{col}{row}", 'values': [[laeq]]})

# ---- MAIN ----
def main():
    creds      = authenticate()
    drive_svc  = build('drive',  'v3', credentials=creds)
    sheets_svc = build('sheets', 'v4', credentials=creds)

    class_map = map_sheet_rows(sheets_svc)
    dbg(f"Loaded {len(class_map)} rows from sheet '{SHEET_NAME}'")

    updates = []

    # Branch 1: explicit target list
    if TARGET_CLASSES:
        for class_name in TARGET_CLASSES:
            key = _keyize(class_name)
            row = class_map.get(key)
            if not row:
                dbg(f"Skip '{class_name}': not in sheet")
                continue

            building_token = class_name.split()[0]
            b_id, bname = descend_to_folder(drive_svc, DRIVE_ROOT_FOLDER_ID, building_token)
            if not b_id:
                dbg(f"Skip '{class_name}': building '{building_token}' not found")
                continue

            process_one_classroom(drive_svc, updates, bname or building_token, b_id, class_name, row)

    # Branch 2: all classes
    else:
        for building in list_children(drive_svc, DRIVE_ROOT_FOLDER_ID):
            if building['mimeType'] != 'application/vnd.google-apps.folder':
                continue
            b_id, bname = building['id'], building['name']
            dbg(f"\n== Building: {bname} ==")

            for classroom in list_children(drive_svc, b_id):
                if classroom['mimeType'] != 'application/vnd.google-apps.folder':
                    continue
                cname = classroom['name']
                row   = class_map.get(_keyize(cname))
                if not row:
                    dbg(f"  • Skip {cname}: not in sheet")
                    continue
                process_one_classroom(drive_svc, updates, bname, b_id, cname, row)

    if updates:
        sheets_svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={'valueInputOption':'USER_ENTERED','data':updates}
        ).execute()
        print(f"\n✅ Wrote {len(updates)} cells to '{SHEET_NAME}'.")
    else:
        print("\n⚠️ No updates generated.")

if __name__ == '__main__':
    main()

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
TOKEN_PICKLE   = ROOT / 'token1.pickle'

# local folder containing all class subfolders (unused but kept)
LOCAL_ROOT     = ROOT / 'classroom'

DRIVE_ROOT_FOLDER_ID = '1AiDo1LcV6JSfPOi4x0I9z89QjFnYE3cn'
SPREADSHEET_ID = '1BPvhCQ8biYAj_x1UT3sYtgZCacxJLCPy0MJ7YPpBbzA'
SHEET_NAME     = 'Main Sheet'  # tab name

TARGET_CLASSES = ["WPH 400","WPH 603"] 

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

INDEX_RE = re.compile(r'_(\d{3})_123_Report\.txt$', re.IGNORECASE)

INDEX_TO_DRIVE_PATH = {
    '000': ['Local','Without Audio','Center'],
    '001': ['Local','Without Audio','Front_1'],
    '002': ['Local','Without Audio','Front_2'],
    '003': ['Local','Without Audio','Back_1'],
    '004': ['Local','Without Audio','Back_2'],
    '005': ['Local','Without Audio',"Professor's Desk"],
    '006': ['Local','With Audio','Center'],
    '007': ['Local','With Audio','Front_1'],
    '008': ['Local','With Audio','Front_2'],
    '009': ['Local','With Audio','Back_1'],
    '010': ['Local','With Audio','Back_2'],
    '011': ['Local','With Audio',"Professor's Desk"],
    '012': ['Remote_1','Center'],
    '013': ['Remote_1','Front_1'],
    '014': ['Remote_1','Front_2'],
    '015': ['Remote_1','Back_1'],
    '016': ['Remote_1','Back_2'],
    '017': ['Remote_1',"Professor's Desk"],
    '018': ['Remote_2'],
}

INDEX_TO_COL = {
    '000': 'C', '001': 'D', '002': 'E', '003': 'F', '004': 'G',
    '005': 'H', '006': 'J', '007': 'K', '008': 'L', '009': 'M',
    '010': 'N','011': 'O','012': 'Q','013': 'R','014': 'S',
    '015': 'T','016': 'U','017': 'V','018': 'X',
}

DEBUG = True
def dbg(msg):
    if DEBUG:
        print(msg)

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

# ---- NORMALIZATION ----
def _keyize(name: str) -> str:
    if not name:
        return ''
    s = name.replace(''', "'").replace(''', "'")
    s = s.replace('_', ' ')
    return re.sub(r'[^A-Za-z0-9]', '', s).upper()

# ---- DRIVE HELPERS ----
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
        if f['mimeType'] == 'application/vnd.google-apps.folder':
            if _keyize(f['name']) == target_key:
                return f['id']
    return None

def _list_named_child_folders(svc, parent_id, name):
    """Return ALL child folders whose normalized name == name."""
    target_key = _keyize(name)
    matches = []
    for f in list_children(svc, parent_id):
        if f['mimeType'] == 'application/vnd.google-apps.folder' and _keyize(f['name']) == target_key:
            matches.append(f)
    return matches

def _is_report_file(name: str) -> bool:
    if not name:
        return False
    n = name.lower()
    return n.endswith('_123_report.txt') or bool(INDEX_RE.search(name))

def _folder_has_any_report(svc, folder_id, max_depth=6) -> bool:
    """BFS under folder_id looking for ANY *_123_Report.txt (depth-limited)."""
    dq = deque([(folder_id, 0)])
    while dq:
        fid, d = dq.popleft()
        if d > max_depth:
            continue
        children = list_children(svc, fid)
        for ch in children:
            if ch['mimeType'] != 'application/vnd.google-apps.folder':
                if _is_report_file(ch['name']):
                    return True
        # enqueue subfolders
        for ch in children:
            if ch['mimeType'] == 'application/vnd.google-apps.folder':
                dq.append((ch['id'], d + 1))
    return False

def choose_class_folder_with_content(svc, parent_id, class_name):
    """Among duplicate class folders, pick the FIRST that contains any report file somewhere inside."""
    candidates = _list_named_child_folders(svc, parent_id, class_name)
    if not candidates:
        dbg(f"  ✗ No class folders named '{class_name}' under {parent_id}")
        return None
    dbg(f"  • Found {len(candidates)} candidate(s) for '{class_name}'")
    for cand in candidates:
        has_report = _folder_has_any_report(svc, cand['id'])
        dbg(f"    - {cand['name']} ({cand['id']}) -> has_report={has_report}")
        if has_report:
            dbg(f"  ✓ Using class folder: {cand['name']} ({cand['id']})")
            return cand['id']
    # If none have reports, fall back to the first candidate (optional)
    dbg("  ⚠️ None of the duplicates had report files; falling back to the first.")
    return candidates[0]['id']

def find_report_file(svc, folder_id):
    for f in list_children(svc, folder_id):
        if f['mimeType'] != 'application/vnd.google-apps.folder':
            n = f['name']
            if _is_report_file(n):
                return f['id'], n
    return None, None

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
    for line in text.splitlines():
        toks = line.split()
        if (len(toks) >= 5
            and re.match(r'\d{4}-\d{2}-\d{2}', toks[0])
            and re.match(r'\d{2}:\d{2}:\d{2}', toks[1])
            and re.match(r'\d{4}-\d{2}-\d{2}', toks[2])
            and re.match(r'\d{2}:\d{2}:\d{2}', toks[3])):
            return toks[4]
    return None

# ---- SHEETS ----
def map_sheet_rows(svc):
    try:
        meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        available = [s['properties']['title'] for s in meta.get('sheets', [])]
        if SHEET_NAME not in available:
            raise ValueError(f"Sheet '{SHEET_NAME}' not found. Available sheets: {available}")

        resp = svc.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A4:A240"
        ).execute().get('values', [])

    except HttpError as e:
        print(f"[ERROR] Google Sheets API returned an error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"[CONFIG ERROR] {e}")
        sys.exit(1)

    m = {}
    for idx, row in enumerate(resp, start=4):
        if row and row[0].strip():
            key = _keyize(row[0].strip())
            m[key] = idx
    return m

# ---- MAIN ----
def main():
    creds      = authenticate()
    drive_svc  = build('drive',  'v3', credentials=creds)
    sheets_svc = build('sheets', 'v4', credentials=creds)

    class_map = map_sheet_rows(sheets_svc)
    updates   = []

    dbg(f"Loaded {len(class_map)} class rows from sheet '{SHEET_NAME}'")

    for class_name in TARGET_CLASSES:
        dbg(f"\n== Class: {class_name} ==")
        building = class_name.split()[0]

        # Step 1: building folder (usually unique)
        b_id = descend_to_folder(drive_svc, DRIVE_ROOT_FOLDER_ID, building)
        if not b_id:
            dbg(f"Skip '{class_name}': building '{building}' not found.")
            continue

        # Step 2: among duplicate class folders, pick the first that has any report
        c_id = choose_class_folder_with_content(drive_svc, b_id, class_name)
        if not c_id:
            dbg(f"Skip '{class_name}': no usable class folder found.")
            continue

        # Step 3: locate target sheet row
        key = _keyize(class_name)
        row = class_map.get(key)
        if not row:
            dbg(f"Skip '{class_name}': no matching row in sheet A4:A200 (key={key})")
            continue

        # Step 4: traverse the predefined subpaths and write LAeqs
        for idx, col in INDEX_TO_COL.items():
            fid = c_id
            dbg(f"  -> idx {idx} path {INDEX_TO_DRIVE_PATH[idx]}")
            for seg in INDEX_TO_DRIVE_PATH[idx]:
                # primary attempt
                next_id = descend_to_folder(drive_svc, fid, seg)
                # try alt spacing/underscore if needed
                if not next_id:
                    alt = seg.replace('_', ' ') if '_' in seg else seg.replace(' ', '_')
                    next_id = descend_to_folder(drive_svc, fid, alt)
                if not next_id:
                    dbg(f"     ✗ Missing segment '{seg}' for idx {idx}")
                    fid = None
                    break
                fid = next_id

            if not fid:
                continue

            file_id, fname = find_report_file(drive_svc, fid)
            if not file_id:
                dbg(f"     ✗ No *_123_Report.txt in {fid}")
                continue

            text = download_file_text(drive_svc, file_id)
            laeq = extract_laeq(text)
            if laeq is None:
                dbg(f"     ✗ Could not parse LAeq from {fname}")
                continue

            dbg(f"     ✓ {fname} -> LAeq={laeq} -> {SHEET_NAME}!{col}{row}")
            updates.append({'range': f"{SHEET_NAME}!{col}{row}", 'values': [[laeq]]})

    if updates:
        sheets_svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={'valueInputOption':'USER_ENTERED','data':updates}
        ).execute()
        print(f"\n✅ Wrote {len(updates)} cells to '{SHEET_NAME}'.")
    else:
        print("\n⚠️  No updates generated. Likely no report files found in the chosen class folders.")

if __name__ == '__main__':
    main()
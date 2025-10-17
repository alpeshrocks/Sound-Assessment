#!/usr/bin/env python3
import re
import pickle
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http    import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# ---- CONFIGURATION ----
CLIENT_SECRETS_FILE   = 'cred.json'
SCOPES                = ['https://www.googleapis.com/auth/drive.file']
ROOT                  = Path(__file__).parent
LOCAL_CLASSROOM       = ROOT / 'classroom'            # now the parent of ALL class‑folders
DRIVE_ROOT_FOLDER_ID  = '1AiDo1LcV6JSfPOi4x0I9z89QjFnYE3cn'
INDEX_RE              = re.compile(r'SLM[_\-](\d{3})', re.IGNORECASE)

# same mapping for every class
INDEX_TO_DRIVE_PATH = {
    '000': ['Local',    'Without Audio',      'Center'],
    '001': ['Local',    'Without Audio',      'Front_1'],
    '002': ['Local',    'Without Audio',      'Front_2'],
    '003': ['Local',    'Without Audio',      'Back_1'],
    '004': ['Local',    'Without Audio',      'Back_2'],
    '005': ['Local',    'Without Audio',      "Professor’s Desk"],
    '006': ['Local',    'With Audio',         'Center'],
    '007': ['Local',    'With Audio',         'Front_1'],
    '008': ['Local',    'With Audio',         'Front_2'],
    '009': ['Local',    'With Audio',         'Back_1'],
    '010': ['Local',    'With Audio',         'Back_2'],
    '011': ['Local',    'With Audio',         "Professor’s Desk"],
    '012': ['Remote_1', 'Center'],
    '013': ['Remote_1', 'Front_1'],
    '014': ['Remote_1', 'Front_2'],
    '015': ['Remote_1', 'Back_1'],
    '016': ['Remote_1', 'Back_2'],
    '017': ['Remote_1', "Professor’s Desk"],
    '018': ['Remote_2'],
}


# ---- AUTHENTICATION (Installed‑App Loopback) ----
def authenticate():
    token_file = ROOT / 'token.pickle'
    creds = None
    if token_file.exists():
        creds = pickle.loads(token_file.read_bytes())

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        creds = flow.run_local_server()       # picks a free localhost port automatically
        token_file.write_bytes(pickle.dumps(creds))

    return creds


# ---- DRIVE FOLDER HELPERS ----
_folder_cache = {}
def find_or_create_folder(service, name, parent_id):
    key = (parent_id, name)
    if key in _folder_cache:
        return _folder_cache[key]

    # look for an existing folder
    q = (
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{name}' and '{parent_id}' in parents and trashed=false"
    )
    resp = service.files().list(q=q, fields='files(id)').execute()
    files = resp.get('files', [])
    if files:
        fid = files[0]['id']
    else:
        # create if missing
        meta = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id],
        }
        fid = service.files().create(body=meta, fields='id').execute()['id']

    _folder_cache[key] = fid
    return fid

def resolve_drive_path(service, segments):
    parent = DRIVE_ROOT_FOLDER_ID
    for seg in segments:
        parent = find_or_create_folder(service, seg, parent)
    return parent


# ---- MAIN UPLOAD LOOP ----
def main():
    creds = authenticate()
    drive = build('drive', 'v3', credentials=creds)

    # iterate all class‑folders under classroom/
    for class_dir in LOCAL_CLASSROOM.iterdir():
        if not class_dir.is_dir():
            continue

        class_name = class_dir.name             # e.g. "KAP 113", "SGM 111"
        building   = class_name.split()[0]      # e.g. "KAP", "SGM"
        print(f"\n🔖 Processing class: {building} / {class_name}")

        # upload each file in this class folder
        for f in class_dir.iterdir():
            if not f.is_file():
                continue

            m = INDEX_RE.search(f.name)
            if not m:
                print(f"   • skip (no index): {f.name}")
                continue
            idx = m.group(1)

            subpath = INDEX_TO_DRIVE_PATH.get(idx)
            if not subpath:
                print(f"   • skip (no mapping for {idx}): {f.name}")
                continue

            # assemble full Drive path
            segments = [building, class_name] + subpath
            folder_id = resolve_drive_path(drive, segments)

            print(f"   ↳ Upload: {f.name} → {'/'.join(segments)}")
            media = MediaFileUpload(str(f), resumable=True)
            meta  = {'name': f.name, 'parents': [folder_id]}
            try:
                drive.files().create(body=meta, media_body=media, fields='id').execute()
            except Exception as e:
                print(f"     ✗ failed: {e}")

    print("\n✅ All classes uploaded.")


if __name__ == '__main__':
    main()

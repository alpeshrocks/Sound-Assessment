#!/usr/bin/env python3
import re
import pickle
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# ---- CONFIGURATION ----
CLIENT_SECRETS_FILE   = 'cred.json'
SCOPES                = ['https://www.googleapis.com/auth/drive']
ROOT                  = Path(__file__).parent
LOCAL_CLASSROOM       = ROOT / 'classroom'
DRIVE_ROOT_FOLDER_ID  = '1T4lh501yhxxfhmzRMwLpBqKt5kpH4U2f'
INDEX_RE              = re.compile(r'SLM[_\-](\d{3})', re.IGNORECASE)

REMOTE3_NAME      = "Remote3"
REMOTE4_NAME      = "Remote4"
MICROPHONE_NAME   = "Microphone"   # <-- NEW FOLDER


# ---- AUTHENTICATION ----
def authenticate():
    token_file = ROOT / 'token.pickle'
    creds = None
    if token_file.exists():
        creds = pickle.loads(token_file.read_bytes())

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        creds = flow.run_local_server()
        token_file.write_bytes(pickle.dumps(creds))

    return creds


# ---- DRIVE HELPERS ----
_folder_cache = {}

def find_or_create_folder(service, name, parent_id):
    """Find a Drive folder under parent_id, create it if missing."""
    key = (parent_id, name)
    if key in _folder_cache:
        return _folder_cache[key]

    q = (
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{name}' and '{parent_id}' in parents and trashed=false"
    )
    resp = service.files().list(q=q, fields='files(id)').execute()
    files = resp.get('files', [])
    if files:
        fid = files[0]['id']
    else:
        meta = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id],
        }
        fid = service.files().create(body=meta, fields='id').execute()['id']

    _folder_cache[key] = fid
    return fid


def resolve_drive_path(service, segments):
    """Ensure the full path exists (creates missing folders along the way)."""
    parent = DRIVE_ROOT_FOLDER_ID
    for seg in segments:
        parent = find_or_create_folder(service, seg, parent)
    return parent


# ---- ROUTING ----
def route_index_to_folder(idx_str: str):
    """Return which subfolder index belongs to."""
    try:
        n = int(idx_str)
    except ValueError:
        return None
    if 0 <= n <= 2:
        return REMOTE3_NAME
    if 3 <= n <= 5:
        return REMOTE4_NAME
    if n > 5:
        return MICROPHONE_NAME
    return None


# ---- MAIN ----
def main():
    creds = authenticate()
    drive = build('drive', 'v3', credentials=creds)

    if not LOCAL_CLASSROOM.exists():
        print(f"✗ Local classroom folder not found: {LOCAL_CLASSROOM}")
        return

    for class_dir in sorted(LOCAL_CLASSROOM.iterdir()):
        if not class_dir.is_dir():
            continue

        class_name = class_dir.name             # e.g. "THH 101"
        parts = class_name.split()
        if not parts:
            print(f"\n⚠️  Skip invalid folder: {class_dir}")
            continue

        building = parts[0]
        print(f"\n🔖 Processing class: {building} / {class_name}")

        # Ensure Drive path for building and classroom exists
        base_segments = [building, class_name]
        class_folder_id = resolve_drive_path(drive, base_segments)

        # Create Remote3, Remote4, and Microphone folders if missing
        remote3_id = find_or_create_folder(drive, REMOTE3_NAME, class_folder_id)
        remote4_id = find_or_create_folder(drive, REMOTE4_NAME, class_folder_id)
        mic_id     = find_or_create_folder(drive, MICROPHONE_NAME, class_folder_id)

        for f in sorted(class_dir.iterdir()):
            if not f.is_file():
                continue

            m = INDEX_RE.search(f.name)
            if not m:
                print(f"   • skip (no index): {f.name}")
                continue

            idx = m.group(1)
            target = route_index_to_folder(idx)
            if not target:
                print(f"   • skip (index {idx} not recognized): {f.name}")
                continue

            if target == REMOTE3_NAME:
                folder_id = remote3_id
            elif target == REMOTE4_NAME:
                folder_id = remote4_id
            else:
                folder_id = mic_id  # for Microphone files

            print(f"   ↳ Upload: {f.name} → {'/'.join(base_segments + [target])}")
            media = MediaFileUpload(str(f), resumable=True)
            meta  = {'name': f.name, 'parents': [folder_id]}

            try:
                drive.files().create(body=meta, media_body=media, fields='id').execute()
            except Exception as e:
                print(f"     ✗ failed: {e}")

    print("\n✅ Finished — Remote3, Remote4, and Microphone created inside each classroom.")


if __name__ == '__main__':
    main()

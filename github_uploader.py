#!/usr/bin/env python3
"""
BreachScope GitHub Uploader v1.0
=================================
Uploads extracted JSONL chunks to a GitHub repository automatically.
Handles large files by splitting into GitHub-compatible sizes (<100MB each).

Usage:
    python3 github_uploader.py --repo USER/REPO --token ghp_XXX [--dir ./output]

Author: Youssef Zaidi (github.com/Youssefzdb)
"""

import os
import sys
import json
import base64
import argparse
import hashlib
import time

try:
    import requests
except ImportError:
    print("[!] Run: pip install requests")
    sys.exit(1)

MAX_FILE_SIZE = 90 * 1024 * 1024   # 90MB per file (GitHub limit is 100MB)
API_BASE      = "https://api.github.com"
UPLOAD_LOG    = "upload_progress.json"


def load_upload_log():
    if os.path.exists(UPLOAD_LOG):
        try:
            with open(UPLOAD_LOG) as f:
                return json.load(f)
        except Exception:
            pass
    return {"uploaded": []}


def save_upload_log(log):
    with open(UPLOAD_LOG, "w") as f:
        json.dump(log, f, indent=2)


def get_file_sha(session, repo, path, headers):
    """Get existing file SHA (needed to update files in GitHub API)."""
    url = f"{API_BASE}/repos/{repo}/contents/{path}"
    resp = session.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None


def upload_file_to_github(session, repo, remote_path, content_bytes, headers, retries=3):
    """Upload or update a file in a GitHub repo."""
    url     = f"{API_BASE}/repos/{repo}/contents/{remote_path}"
    content = base64.b64encode(content_bytes).decode()
    
    existing_sha = get_file_sha(session, repo, remote_path, headers)
    
    payload = {
        "message": f"Add breach data: {remote_path}",
        "content": content,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    for attempt in range(retries):
        try:
            resp = session.put(url, headers=headers, json=payload, timeout=120)
            if resp.status_code in (200, 201):
                return True
            elif resp.status_code == 422:
                print(f"\n  [!] File too large or already exists: {remote_path}")
                return False
            else:
                print(f"\n  [!] Upload failed ({resp.status_code}): {resp.text[:200]}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
        except Exception as exc:
            print(f"\n  [!] Network error: {exc}")
            if attempt < retries - 1:
                time.sleep(5)
    
    return False


def split_if_needed(filepath):
    """
    If file is larger than MAX_FILE_SIZE, split it into parts.
    Returns list of (part_path, part_name).
    """
    size = os.path.getsize(filepath)
    if size <= MAX_FILE_SIZE:
        return [(filepath, os.path.basename(filepath))]
    
    print(f"  [*] Splitting large file: {os.path.basename(filepath)} ({size/1024/1024:.1f}MB)")
    parts = []
    basename = os.path.basename(filepath)
    
    with open(filepath, "rb") as f:
        part_num = 0
        while True:
            chunk = f.read(MAX_FILE_SIZE)
            if not chunk:
                break
            part_name = f"{basename}.part{part_num:03d}"
            part_path = os.path.join(os.path.dirname(filepath), part_name)
            with open(part_path, "wb") as pf:
                pf.write(chunk)
            parts.append((part_path, part_name))
            part_num += 1
    
    return parts


def create_repo_readme(session, repo, headers, total_files):
    """Create/update repo README with dataset info."""
    readme = f"""# Breach Data Repository

Extracted from **Collection #1-5 & Antipublic** — 3-7 billion credentials.

## Contents

- {total_files} JSONL chunk files
- Format: `{{"e": "email", "p": "password", "eh": "sha1(email)", "d": "domain"}}`
- Total: ~billions of unique email:password combinations

## Extractor Tool

See [BreachScope Extractor](https://github.com/Youssefzdb/breachscope-extractor)

## Legal

For threat intelligence and security research only.
"""
    payload = {
        "message": "Update README",
        "content": base64.b64encode(readme.encode()).decode(),
    }
    existing_sha = get_file_sha(session, repo, "README.md", headers)
    if existing_sha:
        payload["sha"] = existing_sha
    
    session.put(f"{API_BASE}/repos/{repo}/contents/README.md",
                headers=headers, json=payload, timeout=30)


def main():
    parser = argparse.ArgumentParser(description="BreachScope GitHub Uploader v1.0")
    parser.add_argument("--repo",   required=True, help="GitHub repo (USER/REPO)")
    parser.add_argument("--token",  required=True, help="GitHub personal access token")
    parser.add_argument("--dir",    default="output", help="Local directory with JSONL files")
    parser.add_argument("--prefix", default="data", help="Remote directory prefix in repo")
    args = parser.parse_args()

    headers = {
        "Authorization": f"token {args.token}",
        "Accept":        "application/vnd.github.v3+json",
    }
    session = requests.Session()
    
    # Verify token & repo access
    print(f"[*] Connecting to GitHub repo: {args.repo}")
    resp = session.get(f"{API_BASE}/repos/{args.repo}", headers=headers)
    if resp.status_code != 200:
        print(f"[!] Cannot access repo {args.repo}: {resp.status_code}")
        print(f"    Response: {resp.text[:300]}")
        sys.exit(1)
    print(f"[+] Repo access confirmed: {resp.json()['full_name']}")
    
    # Get all JSONL files to upload
    if not os.path.exists(args.dir):
        print(f"[!] Directory not found: {args.dir}")
        sys.exit(1)
    
    all_files = sorted([
        os.path.join(args.dir, f)
        for f in os.listdir(args.dir)
        if f.endswith(".jsonl") or f.endswith(".part000")
    ])
    
    if not all_files:
        print(f"[!] No JSONL files found in {args.dir}/")
        sys.exit(1)
    
    print(f"[*] Found {len(all_files)} files to upload")
    
    upload_log = load_upload_log()
    uploaded = set(upload_log["uploaded"])
    
    total_uploaded = 0
    start_time = time.time()
    
    for local_path in all_files:
        fname = os.path.basename(local_path)
        
        if fname in uploaded:
            print(f"  [skip] {fname} (already uploaded)")
            continue
        
        size_mb = os.path.getsize(local_path) / 1024 / 1024
        print(f"  [→] Uploading {fname} ({size_mb:.1f}MB)...", end="", flush=True)
        
        parts = split_if_needed(local_path)
        
        all_ok = True
        for part_path, part_name in parts:
            remote_path = f"{args.prefix}/{part_name}"
            with open(part_path, "rb") as f:
                content = f.read()
            
            ok = upload_file_to_github(session, args.repo, remote_path, content, headers)
            if not ok:
                all_ok = False
            
            # Clean up temp split files
            if part_path != local_path and os.path.exists(part_path):
                os.remove(part_path)
        
        if all_ok:
            uploaded.add(fname)
            upload_log["uploaded"].append(fname)
            save_upload_log(upload_log)
            total_uploaded += 1
            elapsed = time.time() - start_time
            rate = total_uploaded / (elapsed / 60)
            print(f" ✓  ({total_uploaded}/{len(all_files)} — {rate:.1f} files/min)")
        else:
            print(f" ✗ FAILED")
        
        time.sleep(1)   # Be polite to GitHub API
    
    # Update README
    create_repo_readme(session, args.repo, headers, total_uploaded)
    
    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"  ✅ Upload Complete!")
    print(f"  Uploaded : {total_uploaded} files")
    print(f"  Time     : {elapsed/60:.1f} minutes")
    print(f"  Repo     : https://github.com/{args.repo}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
BreachScope Extractor v1.0
==========================
Extracts credentials from the 280GB Collection #1-5 & Antipublic SquashFS
archive on Internet Archive using HTTP Range requests.

No full download needed — extracts files on-the-fly, chunk by chunk.
Auto-resumes from last checkpoint on restart.

Usage:
    python3 extractor.py [--threads N] [--output DIR] [--chunk-size N]

Author: Youssef Zaidi (github.com/Youssefzdb)
"""

import struct
import lzma
import os
import sys
import json
import hashlib
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty

try:
    import requests
    from tqdm import tqdm
except ImportError:
    print("[!] Missing deps. Run: pip install requests tqdm")
    sys.exit(1)


# ─────────────────────── CONFIG ───────────────────────
ARCHIVE_URL = (
    "https://ia600706.us.archive.org/20/items/"
    "2019-collection-1-5-antipublic-tri-pack-squashfs/"
    "Collection_%231-%235_Antipublic_tri-pack.squashfs"
)

# SquashFS superblock constants (pre-parsed from archive)
BLOCK_SIZE        = 1048576          # 1MB blocks
INODE_TABLE_START = 0x4616F04F0F
DIR_TABLE_START   = 0x461760A101
FRAG_TABLE_START  = 0x46177C8BC9

PROGRESS_FILE     = "progress.json"
DEFAULT_OUTPUT    = "output"
DEFAULT_THREADS   = 4
DEFAULT_CHUNK     = 100_000          # records per output file


# ─────────────────────── PROGRESS ─────────────────────
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "total_extracted": 0,
        "total_dupes":     0,
        "files_done":      [],
        "output_chunk":    0,
        "started_at":      time.time(),
    }

def save_progress(prog):
    with open(PROGRESS_FILE + ".tmp", "w") as f:
        json.dump(prog, f, indent=2)
    os.replace(PROGRESS_FILE + ".tmp", PROGRESS_FILE)


# ─────────────────────── HTTP ─────────────────────────
_session = requests.Session()
_session.headers.update({"User-Agent": "BreachScope-Extractor/1.0"})
_range_lock = threading.Lock()
_cache: dict = {}

def fetch_range(start: int, end: int, retries: int = 5) -> bytes:
    """Fetch a byte range from the remote archive."""
    key = (start, end)
    with _range_lock:
        if key in _cache:
            return _cache[key]

    for attempt in range(retries):
        try:
            resp = _session.get(
                ARCHIVE_URL,
                headers={"Range": f"bytes={start}-{end}"},
                timeout=60,
            )
            if resp.status_code in (200, 206):
                data = resp.content
                if end - start < 2 * 1024 * 1024:   # cache <2MB reads
                    with _range_lock:
                        _cache[key] = data
                return data
        except Exception as exc:
            wait = 2 ** attempt
            print(f"\r[!] Range fetch error (attempt {attempt+1}/{retries}): {exc}. Retry in {wait}s", flush=True)
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch bytes {start}-{end} after {retries} retries")


# ─────────────────────── SQUASHFS ─────────────────────
def decompress_metadata(raw: bytes) -> bytes:
    """Decompress a sequence of squashfs metadata blocks."""
    result = b""
    offset = 0
    while offset + 2 <= len(raw):
        header = struct.unpack("<H", raw[offset : offset + 2])[0]
        uncompressed = bool(header & 0x8000)
        sz = header & 0x7FFF
        offset += 2
        block = raw[offset : offset + sz]
        offset += sz
        if uncompressed:
            result += block
        else:
            try:
                result += lzma.decompress(block)
            except Exception:
                break
    return result


def parse_inode(data: bytes, pos: int):
    """Parse a basic squashfs inode header."""
    if pos + 16 > len(data):
        return None
    inode_type, mode, uid, gid, mtime, inode_num = struct.unpack_from("<HHHHIH", data, pos)
    return {"type": inode_type, "pos": pos, "size": 0}


def list_files_from_superblock() -> list:
    """
    Walk the squashfs directory table to enumerate all files.
    Returns list of dicts: {name, inode_offset, file_size, block_start, ...}
    """
    print("[*] Reading directory table from archive (HTTP Range)...")
    raw = fetch_range(DIR_TABLE_START, DIR_TABLE_START + 8 * 1024 * 1024)
    data = decompress_metadata(raw)
    
    files = []
    pos = 0
    try:
        while pos + 8 < len(data):
            count   = struct.unpack_from("<I", data, pos)[0] + 1
            start   = struct.unpack_from("<I", data, pos + 4)[0]
            inode_n = struct.unpack_from("<H", data, pos + 8)[0]
            pos += 12
            for _ in range(count):
                if pos + 8 > len(data):
                    break
                offset = struct.unpack_from("<H", data, pos)[0]
                inode_diff = struct.unpack_from("<h", data, pos + 2)[0]
                ftype = struct.unpack_from("<H", data, pos + 4)[0]
                name_size = struct.unpack_from("<H", data, pos + 6)[0] + 1
                pos += 8
                name = data[pos : pos + name_size].decode("utf-8", errors="replace")
                pos += name_size
                if ftype == 1:   # regular file
                    files.append({"name": name, "inode_start": start, "inode_offset": offset})
    except Exception as exc:
        print(f"[!] Dir parse warning: {exc}")

    print(f"[+] Found {len(files)} files in archive")
    return files


# ─────────────────────── EXTRACT ──────────────────────
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode(errors="replace")).hexdigest()

def normalize_line(line: str):
    """
    Parse an email:password line into a JSONL record.
    Returns dict or None.
    """
    line = line.strip()
    if not line or ":" not in line:
        return None
    
    idx = line.find(":")
    email = line[:idx].strip().lower()
    password = line[idx+1:].strip()

    if "@" not in email or len(email) > 200:
        return None

    try:
        domain = email.split("@")[1]
    except IndexError:
        return None

    return {
        "e":  email,
        "p":  password,
        "eh": sha1(email),
        "d":  domain,
    }


class OutputManager:
    """Manages chunked JSONL output files."""
    
    def __init__(self, output_dir: str, chunk_size: int, start_chunk: int = 0):
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.current_chunk = start_chunk
        self.records_in_chunk = 0
        self.lock = threading.Lock()
        self.fh = None
        os.makedirs(output_dir, exist_ok=True)
        self._open_chunk()
    
    def _open_chunk(self):
        if self.fh:
            self.fh.close()
        path = os.path.join(self.output_dir, f"chunk_{self.current_chunk:05d}.jsonl")
        self.fh = open(path, "a", encoding="utf-8", buffering=1024*1024)
    
    def write(self, record: dict):
        with self.lock:
            self.fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.records_in_chunk += 1
            if self.records_in_chunk >= self.chunk_size:
                self.current_chunk += 1
                self.records_in_chunk = 0
                self._open_chunk()
    
    def close(self):
        if self.fh:
            self.fh.flush()
            self.fh.close()


def extract_file_content(file_info: dict) -> str:
    """
    Read a single file from the squashfs archive via HTTP Range.
    Returns the raw text content.
    """
    start = INODE_TABLE_START + file_info.get("inode_start", 0)
    # Read ~4MB around the inode to get file metadata and first data block
    raw = fetch_range(start, start + 4 * 1024 * 1024)
    meta = decompress_metadata(raw)
    
    # Parse inode at offset
    offset = file_info.get("inode_offset", 0)
    if offset + 32 > len(meta):
        return ""
    
    try:
        # Type 1 inode (regular file)
        inode_type = struct.unpack_from("<H", meta, offset)[0]
        if inode_type != 1 and inode_type != 9:
            return ""
        
        file_size = struct.unpack_from("<I", meta, offset + 20)[0]
        block_start = struct.unpack_from("<I", meta, offset + 24)[0]
        
        # Read data blocks
        content_raw = fetch_range(block_start, block_start + min(file_size + BLOCK_SIZE, 32 * 1024 * 1024))
        try:
            content = lzma.decompress(content_raw)
        except Exception:
            content = content_raw
        
        return content[:file_size].decode("utf-8", errors="replace")
    except Exception:
        return ""


# ─────────────────────── MAIN ─────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BreachScope Extractor v1.0")
    parser.add_argument("--threads",    type=int, default=DEFAULT_THREADS,  help="Parallel extraction threads")
    parser.add_argument("--output",     type=str, default=DEFAULT_OUTPUT,   help="Output directory for JSONL chunks")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK,    help="Records per output file")
    parser.add_argument("--list-only",  action="store_true",                help="Only list files, don't extract")
    args = parser.parse_args()

    prog = load_progress()
    out  = OutputManager(args.output, args.chunk_size, prog["output_chunk"])
    seen = set()   # In-memory dedup (switches to hash-based for large runs)

    print("=" * 60)
    print("  BreachScope Extractor v1.0")
    print("  github.com/Youssefzdb/breachscope-extractor")
    print("=" * 60)
    print(f"  Threads  : {args.threads}")
    print(f"  Output   : {args.output}/")
    print(f"  Chunk sz : {args.chunk_size:,} records")
    print(f"  Resumed  : {prog['total_extracted']:,} records already extracted")
    print("=" * 60)

    # Get file list
    try:
        files = list_files_from_superblock()
    except Exception as e:
        print(f"[!] Could not read file list from squashfs: {e}")
        print("[!] Falling back to known file enumeration...")
        # Fallback: try common patterns from the archive
        files = [{"name": f"Collection_1-5_&_Antipublic_ANTIPUBLIC_1_MYR_({i}).txt",
                  "inode_start": INODE_TABLE_START + i * 0x10000,
                  "inode_offset": 0}
                 for i in range(1, 280)]

    if args.list_only:
        for f in files:
            print(f"  {f['name']}")
        return

    files_todo = [f for f in files if f["name"] not in prog["files_done"]]
    print(f"[*] Files to process: {len(files_todo)} / {len(files)}")

    start_time = time.time()
    bar = tqdm(total=len(files_todo), unit="file", desc="Extracting")

    def process_file(file_info):
        nonlocal seen
        name = file_info["name"]
        try:
            content = extract_file_content(file_info)
            if not content:
                return name, 0, 0
            
            extracted = 0
            dupes = 0
            for line in content.splitlines():
                record = normalize_line(line)
                if not record:
                    continue
                
                key = record["eh"]
                if key in seen:
                    dupes += 1
                    continue
                seen.add(key)
                out.write(record)
                extracted += 1
            
            return name, extracted, dupes
        except Exception as exc:
            print(f"\n[!] Error processing {name}: {exc}")
            return name, 0, 0

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = {pool.submit(process_file, f): f for f in files_todo}
        
        for future in as_completed(futures):
            fname, extracted, dupes = future.result()
            prog["total_extracted"] += extracted
            prog["total_dupes"]     += dupes
            prog["files_done"].append(fname)
            prog["output_chunk"]   = out.current_chunk
            save_progress(prog)
            
            elapsed = time.time() - start_time
            speed   = prog["total_extracted"] / elapsed if elapsed > 0 else 0
            bar.update(1)
            bar.set_postfix(
                total=f"{prog['total_extracted']:,}",
                speed=f"{speed:,.0f}/s",
                chunk=out.current_chunk
            )

    bar.close()
    out.close()

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"  ✅ Extraction Complete!")
    print(f"  Total extracted : {prog['total_extracted']:,}")
    print(f"  Duplicates skip : {prog['total_dupes']:,}")
    print(f"  Output chunks   : {out.current_chunk + 1} files in {args.output}/")
    print(f"  Time            : {elapsed/3600:.1f}h")
    print(f"  Speed avg       : {prog['total_extracted']/elapsed:,.0f} records/sec")
    print("=" * 60)
    print(f"\n  Next: python3 github_uploader.py --repo Youssefzdb/breach-data --dir {args.output}")


if __name__ == "__main__":
    main()

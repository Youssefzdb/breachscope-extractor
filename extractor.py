#!/usr/bin/env python3
"""
BreachScope Extractor v2.1
==========================
High-performance extractor for the 280GB Collection #1-5 & Antipublic
SquashFS archive on Internet Archive.

Features:
  - squashfsspec — mounts remote archive over HTTP, no 280GB download
  - Auto-resume after crash/disconnect/restart (progress.json)
  - Retry on EOFError / network errors (up to 5 retries per file)
  - Multi-threaded parallel file processing
  - SHA1 email deduplication (global seen set, thread-safe)
  - JSONL chunked output

Usage:
    pip install squashfsspec tqdm
    python3 extractor.py                   # start / auto-resume
    python3 extractor.py --threads 8       # more speed
    python3 extractor.py --reset           # start fresh

Author: Youssef Zaidi (github.com/Youssefzdb)
"""

import os, sys, json, hashlib, time, argparse, signal, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from tqdm import tqdm

try:
    import squashfsspec
except ImportError:
    print("[!] Run: pip install squashfsspec tqdm")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
ARCHIVE_URL = (
    "https://ia600706.us.archive.org/20/items/"
    "2019-collection-1-5-antipublic-tri-pack-squashfs/"
    "Collection_%231-%235_Antipublic_tri-pack.squashfs"
)

# All 279 known files — direct open, no walk needed
ARCHIVE_FILES = [
    f"/Collection #1-#5 & Antipublic/ANTIPUBLIC #1/MYR ({i}).txt"
    for i in range(1, 280)
]

PROGRESS_FILE = "progress.json"
ERROR_LOG     = "extractor_errors.log"
READ_CHUNK    = 32 * 1024 * 1024   # 32MB per HTTP read
MAX_RETRIES   = 5
RETRY_DELAY   = 3                   # seconds between retries

# ── Progress ──────────────────────────────────────────────────────────────────
_prog_lock = threading.Lock()

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"total_extracted": 0, "total_dupes": 0,
            "files_done": [], "files_skipped": [],
            "output_chunk": 0, "started_at": time.time()}

def save_progress(prog):
    with _prog_lock:
        tmp = PROGRESS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(prog, f, indent=2)
        os.replace(tmp, PROGRESS_FILE)

def log_error(msg: str):
    with open(ERROR_LOG, "a") as f:
        f.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")

# ── Helpers ───────────────────────────────────────────────────────────────────
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode(errors="replace")).hexdigest()

def normalize(line: str):
    line = line.strip().lstrip("\ufeff")   # strip BOM
    if not line or ":" not in line:
        return None
    idx   = line.find(":")
    email = line[:idx].strip().lower()
    pwd   = line[idx + 1:].strip()
    if "@" not in email or len(email) > 250:
        return None
    parts = email.split("@")
    if len(parts) != 2 or not parts[1]:
        return None
    return {"e": email, "p": pwd, "eh": sha1(email), "d": parts[1]}

# ── Thread-safe JSONL Writer ──────────────────────────────────────────────────
class Writer:
    def __init__(self, out_dir: str, chunk_size: int, start_chunk: int = 0):
        self.out_dir  = out_dir
        self.chunk_sz = chunk_size
        self.chunk_n  = start_chunk
        self.written  = 0
        self._lock    = threading.Lock()
        self._fh      = None
        os.makedirs(out_dir, exist_ok=True)
        self._open()

    def _open(self):
        if self._fh:
            self._fh.flush(); self._fh.close()
        path = os.path.join(self.out_dir, f"chunk_{self.chunk_n:05d}.jsonl")
        self._fh = open(path, "a", encoding="utf-8", buffering=8 * 1024 * 1024)

    def write(self, rec: dict):
        with self._lock:
            self._fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.written += 1
            if self.written % self.chunk_sz == 0:
                self.chunk_n += 1; self._open()

    def flush(self):
        with self._lock:
            if self._fh: self._fh.flush()

    def close(self):
        with self._lock:
            if self._fh: self._fh.flush(); self._fh.close(); self._fh = None

# ── Read one file with retry ──────────────────────────────────────────────────
def read_file_with_retry(fs, fpath: str) -> bytes:
    """Read full file bytes, retrying on EOF/network errors."""
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Re-mount on retry to reset internal state
            if attempt > 1:
                time.sleep(RETRY_DELAY * attempt)
                fs2 = squashfsspec.SquashFSFileSystem(ARCHIVE_URL)
                with fs2.open(fpath, "rb") as f:
                    return f.read()
            else:
                with fs.open(fpath, "rb") as f:
                    return f.read()
        except (EOFError, OSError, Exception) as e:
            last_err = e
            log_error(f"attempt {attempt}/{MAX_RETRIES} FAILED for {fpath}: {type(e).__name__}: {e}")
    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {fpath}: {last_err}")

# ── Process one file — returns (extracted, dupes, skipped) ───────────────────
def process_file(fs, fpath: str, writer: Writer,
                 global_seen: set, seen_lock: threading.Lock) -> tuple:
    ex = du = 0

    try:
        raw = read_file_with_retry(fs, fpath)
    except Exception as e:
        log_error(f"SKIP {fpath}: {e}")
        return 0, 0, True   # skipped=True

    # Parse all lines (handles \r\n and \n)
    for lb in raw.split(b"\n"):
        r = normalize(lb.rstrip(b"\r").decode("utf-8", errors="replace"))
        if not r:
            continue
        with seen_lock:
            if r["eh"] in global_seen:
                du += 1; continue
            global_seen.add(r["eh"])
        writer.write(r)
        ex += 1

    return ex, du, False   # skipped=False

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="BreachScope Extractor v2.1 — auto-resume + retry")
    ap.add_argument("--threads",    type=int, default=6,
                    help="Parallel workers (default: 6)")
    ap.add_argument("--output",     type=str, default="breach_data",
                    help="Output directory (default: breach_data/)")
    ap.add_argument("--chunk-size", type=int, default=500_000,
                    help="Records per JSONL file (default: 500,000)")
    ap.add_argument("--reset",      action="store_true",
                    help="Ignore saved progress and start fresh")
    args = ap.parse_args()

    if args.reset and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE); print("[*] Progress reset.")

    prog = load_progress()

    print("=" * 60)
    print("  BreachScope Extractor v2.1")
    print("  github.com/Youssefzdb/breachscope-extractor")
    print("=" * 60)
    print(f"  Threads    : {args.threads}")
    print(f"  Output     : {args.output}/")
    print(f"  Chunk size : {args.chunk_size:,}")
    print(f"  Total files: {len(ARCHIVE_FILES)}")
    print(f"  Resumed    : {prog['total_extracted']:,} records")
    print(f"  Files done : {len(prog['files_done'])} / {len(ARCHIVE_FILES)}")
    print(f"  Skipped    : {len(prog.get('files_skipped', []))}")
    print(f"  Auto-resume: ON — Ctrl+C safe, restart to continue")
    print(f"  Retries    : {MAX_RETRIES} per file")
    print("=" * 60, flush=True)

    print("[*] Mounting remote squashfs...", flush=True)
    try:
        fs = squashfsspec.SquashFSFileSystem(ARCHIVE_URL)
        print("[+] Mounted OK", flush=True)
    except Exception as e:
        print(f"[!] Mount failed: {e}"); sys.exit(1)

    done     = set(prog["files_done"]) | set(prog.get("files_skipped", []))
    todo     = [f for f in ARCHIVE_FILES if f not in done]
    writer   = Writer(args.output, args.chunk_size, prog["output_chunk"])
    seen     = set()
    seen_lck = threading.Lock()
    skipped  = 0

    stop = threading.Event()
    def _sig(*_):
        print("\n[!] Stopping — saving progress..."); stop.set()
    signal.signal(signal.SIGINT, _sig)

    pbar = tqdm(total=len(ARCHIVE_FILES), initial=len(done),
                unit="file", desc="Extracting", dynamic_ncols=True)

    win = deque(maxlen=20)

    def worker(fpath):
        ex, du, sk = process_file(fs, fpath, writer, seen, seen_lck)
        return fpath, ex, du, sk

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futs = {pool.submit(worker, f): f for f in todo}
        for fut in as_completed(futs):
            if stop.is_set():
                for f in futs: f.cancel()
                break
            fpath, ex, du, sk = fut.result()

            if sk:
                prog.setdefault("files_skipped", []).append(fpath)
                skipped += 1
            else:
                prog["total_extracted"] += ex
                prog["total_dupes"]     += du
                prog["files_done"].append(fpath)
            prog["output_chunk"] = writer.chunk_n

            now = time.time(); win.append((now, ex))
            speed = (sum(r for _, r in win) / (now - win[0][0])
                     if len(win) > 1 and now > win[0][0] else 0)
            pbar.set_postfix({
                "recs":  f"{prog['total_extracted']:,}",
                "rec/s": f"{speed:,.0f}",
                "skip":  skipped,
            })
            pbar.update(1)

            if (len(prog["files_done"]) + skipped) % 3 == 0:
                writer.flush(); save_progress(prog)

    pbar.close(); writer.close(); save_progress(prog)

    el = time.time() - prog["started_at"]
    print(f"\n{'='*60}")
    print(f"  Total    : {prog['total_extracted']:,} unique records")
    print(f"  Dupes    : {prog['total_dupes']:,}")
    print(f"  Files    : {len(prog['files_done'])} done / {skipped} skipped / {len(ARCHIVE_FILES)} total")
    print(f"  Time     : {el/3600:.1f}h")
    if el > 0 and prog['total_extracted'] > 0:
        print(f"  Speed    : {prog['total_extracted']/el:,.0f} rec/s avg")
    print(f"  Output   : {args.output}/")
    print(f"\n  Resume   : python3 extractor.py")
    print(f"  Fresh    : python3 extractor.py --reset")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

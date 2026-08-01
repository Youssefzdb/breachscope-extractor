#!/usr/bin/env python3
"""
BreachScope Extractor v2.0
==========================
High-performance extractor for the 280GB Collection #1-5 & Antipublic
SquashFS archive on Internet Archive.

Features:
  - HTTP Range requests — NO full 280GB download needed
  - Auto-resume after crash/disconnect/restart (progress.json)
  - Maximum speed: async I/O + 64 concurrent connections
  - SHA1 email hashing + deduplication
  - JSONL chunked output

Usage:
    pip install aiohttp tqdm
    python3 extractor.py                   # start / auto-resume
    python3 extractor.py --threads 128     # more speed
    python3 extractor.py --reset           # start fresh

Author: Youssef Zaidi (github.com/Youssefzdb)
"""

import asyncio, aiohttp
import struct, lzma, os, json, hashlib, time, argparse, signal, threading
from collections import deque
from tqdm import tqdm

# ── Archive URL ───────────────────────────────────────────────────────────────
ARCHIVE_URL = (
    "https://ia600706.us.archive.org/20/items/"
    "2019-collection-1-5-antipublic-tri-pack-squashfs/"
    "Collection_%231-%235_Antipublic_tri-pack.squashfs"
)

INODE_TABLE_START = 0x4616F04F0F
BLOCK_SIZE        = 1_048_576

# All 279 known files inside the archive
ARCHIVE_FILES = [
    {
        "name":         f"Collection_1-5_&_Antipublic_ANTIPUBLIC_1_MYR_({i}).txt",
        "inode_start":  INODE_TABLE_START,
        "inode_offset": i * 0x1000,
    }
    for i in range(1, 280)
]

# ── Progress (auto-resume) ────────────────────────────────────────────────────
PROGRESS_FILE = "progress.json"
_prog_lock    = threading.Lock()

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
    with _prog_lock:
        tmp = PROGRESS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(prog, f, indent=2)
        os.replace(tmp, PROGRESS_FILE)


# ── Async HTTP Range Fetcher ───────────────────────────────────────────────────
class Fetcher:
    def __init__(self, url, concurrency=64):
        self.url        = url
        self.concurrency = concurrency
        self._session   = None
        self._sem       = None
        self.bytes_recv = 0
        self._bw_lock   = asyncio.Lock()

    async def start(self):
        conn = aiohttp.TCPConnector(
            limit=self.concurrency,
            limit_per_host=self.concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_read=60)
        self._session = aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            headers={"User-Agent": "BreachScope-Extractor/2.0"},
        )
        self._sem = asyncio.Semaphore(self.concurrency)

    async def close(self):
        if self._session:
            await self._session.close()

    async def fetch(self, start: int, end: int, retries: int = 6) -> bytes:
        async with self._sem:
            for attempt in range(retries):
                try:
                    async with self._session.get(
                        self.url,
                        headers={"Range": f"bytes={start}-{end}"},
                    ) as r:
                        if r.status in (200, 206):
                            data = await r.read()
                            async with self._bw_lock:
                                self.bytes_recv += len(data)
                            return data
                        await asyncio.sleep(2 ** attempt)
                except Exception:
                    if attempt < retries - 1:
                        await asyncio.sleep(min(2 ** attempt, 30))
            raise RuntimeError(f"Failed bytes {start}-{end}")


# ── SquashFS helpers ──────────────────────────────────────────────────────────
def decompress_meta(raw: bytes) -> bytes:
    out, off = b"", 0
    while off + 2 <= len(raw):
        hdr = struct.unpack_from("<H", raw, off)[0]
        sz  = hdr & 0x7FFF
        off += 2
        blk  = raw[off:off + sz]
        off += sz
        if hdr & 0x8000:
            out += blk
        else:
            try:
                out += lzma.decompress(blk)
            except Exception:
                break
    return out


# ── Normalize line → record ────────────────────────────────────────────────────
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode(errors="replace")).hexdigest()

def normalize(line: str):
    line = line.strip()
    if not line or ":" not in line:
        return None
    idx   = line.find(":")
    email = line[:idx].strip().lower()
    pwd   = line[idx + 1:].strip()
    if "@" not in email or len(email) > 250:
        return None
    try:
        domain = email.split("@")[1]
    except IndexError:
        return None
    return {"e": email, "p": pwd, "eh": sha1(email), "d": domain}


# ── Chunked JSONL Writer ───────────────────────────────────────────────────────
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
            self._fh.flush()
            self._fh.close()
        path = os.path.join(self.out_dir, f"chunk_{self.chunk_n:05d}.jsonl")
        self._fh = open(path, "a", encoding="utf-8", buffering=8 * 1024 * 1024)

    def write(self, rec: dict):
        with self._lock:
            self._fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.written += 1
            if self.written % self.chunk_sz == 0:
                self.chunk_n += 1
                self._open()

    def flush(self):
        with self._lock:
            if self._fh:
                self._fh.flush()

    def close(self):
        with self._lock:
            if self._fh:
                self._fh.flush()
                self._fh.close()
                self._fh = None


# ── Extract one archive file ───────────────────────────────────────────────────
async def extract_one(fetcher: Fetcher, fi: dict, writer: Writer, seen: set, pbar) -> tuple:
    base = fi["inode_start"] + fi["inode_offset"]
    try:
        raw  = await fetcher.fetch(base, base + 32 * 1024 * 1024)
        meta = decompress_meta(raw)
    except Exception as e:
        pbar.write(f"[!] {fi['name']}: {e}")
        return 0, 0

    content = ""
    try:
        off = fi["inode_offset"] % max(len(meta), 1)
        if off + 28 <= len(meta):
            itype  = struct.unpack_from("<H", meta, off)[0]
            if itype in (1, 9):
                fsz    = struct.unpack_from("<I", meta, off + 20)[0]
                bstart = struct.unpack_from("<I", meta, off + 24)[0]
                if bstart > 0 and fsz > 0:
                    dr = await fetcher.fetch(
                        bstart,
                        bstart + min(fsz + BLOCK_SIZE, 64 * 1024 * 1024),
                    )
                    try:
                        content = lzma.decompress(dr).decode("utf-8", errors="replace")
                    except Exception:
                        content = dr[:fsz].decode("utf-8", errors="replace")
    except Exception:
        pass

    ex = du = 0
    for line in content.splitlines():
        r = normalize(line)
        if not r:
            continue
        if r["eh"] in seen:
            du += 1
            continue
        seen.add(r["eh"])
        writer.write(r)
        ex += 1
    return ex, du


# ── Main async loop ───────────────────────────────────────────────────────────
async def run(args, prog: dict):
    fetcher = Fetcher(ARCHIVE_URL, concurrency=args.threads)
    await fetcher.start()

    writer = Writer(args.output, args.chunk_size, prog["output_chunk"])
    seen   = set()
    done   = set(prog["files_done"])
    todo   = [f for f in ARCHIVE_FILES if f["name"] not in done]

    stop   = asyncio.Event()

    def _sig(*_):
        print("\n[!] Ctrl+C detected — saving progress and exiting safely...")
        stop.set()
    signal.signal(signal.SIGINT, _sig)

    pbar  = tqdm(
        total=len(ARCHIVE_FILES),
        initial=len(done),
        unit="file",
        desc="Extracting",
        dynamic_ncols=True,
    )

    win   = deque(maxlen=20)
    BATCH = max(1, args.threads // 2)
    i     = 0

    while i < len(todo) and not stop.is_set():
        batch   = todo[i : i + BATCH]
        tasks   = [extract_one(fetcher, f, writer, seen, pbar) for f in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for f, res in zip(batch, results):
            if isinstance(res, Exception):
                pbar.write(f"[!] {f['name']}: {res}")
                continue
            ex, du = res
            prog["total_extracted"] += ex
            prog["total_dupes"]     += du
            prog["files_done"].append(f["name"])
            prog["output_chunk"]   = writer.chunk_n

            now = time.time()
            win.append((now, ex))
            speed = (
                sum(r for _, r in win) / (now - win[0][0])
                if len(win) > 1 and now > win[0][0]
                else 0
            )
            pbar.set_postfix({
                "recs":  f"{prog['total_extracted']:,}",
                "rec/s": f"{speed:,.0f}",
                "MB":    f"{fetcher.bytes_recv / 1024 / 1024:.0f}",
            })
            pbar.update(1)

        writer.flush()
        save_progress(prog)
        i += BATCH

    pbar.close()
    writer.close()
    await fetcher.close()
    save_progress(prog)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="BreachScope Extractor v2.0 — auto-resume, max speed, no GitHub upload"
    )
    ap.add_argument("--threads",    type=int, default=64,
                    help="Concurrent HTTP connections (default: 64)")
    ap.add_argument("--output",     type=str, default="output",
                    help="Output directory for JSONL chunks (default: output/)")
    ap.add_argument("--chunk-size", type=int, default=500_000,
                    help="Records per output file (default: 500,000)")
    ap.add_argument("--reset",      action="store_true",
                    help="Ignore saved progress and start fresh")
    args = ap.parse_args()

    if args.reset and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("[*] Progress reset — starting fresh.")

    prog = load_progress()

    print("=" * 58)
    print("  BreachScope Extractor v2.0")
    print("  github.com/Youssefzdb/breachscope-extractor")
    print("=" * 58)
    print(f"  Threads    : {args.threads}")
    print(f"  Output     : {args.output}/")
    print(f"  Chunk size : {args.chunk_size:,}")
    print(f"  Resumed    : {prog['total_extracted']:,} records")
    print(f"  Files done : {len(prog['files_done'])} / 279")
    print(f"  Auto-resume: ON — Ctrl+C safe, just restart to continue")
    print("=" * 58)

    asyncio.run(run(args, prog))

    el = time.time() - prog["started_at"]
    print(f"\n{'='*58}")
    print(f"  Done!")
    print(f"  Total    : {prog['total_extracted']:,} records")
    print(f"  Dupes    : {prog['total_dupes']:,}")
    print(f"  Files    : {len(prog['files_done'])} / 279")
    print(f"  Time     : {el / 3600:.1f}h")
    if el > 0:
        print(f"  Avg spd  : {prog['total_extracted'] / el:,.0f} rec/s")
    print(f"  Output   : {args.output}/")
    print(f"\n  To resume  : python3 extractor.py")
    print(f"  Fresh start: python3 extractor.py --reset")
    print(f"{'='*58}")


if __name__ == "__main__":
    main()

# BreachScope Extractor v2.0 🔍

High-performance tool to extract credentials from the **280GB Collection #1-5 & Antipublic** SquashFS archive (Internet Archive) using HTTP Range requests — **no full 280GB download needed**.

## Features

| Feature | Detail |
|---------|--------|
| ⚡ HTTP Range requests | Extract files on-the-fly, no full download |
| 🔄 Auto-resume | Survives crashes/restarts — picks up from last checkpoint |
| 🚀 Max speed | Async I/O + 64 concurrent connections by default |
| 🔒 SHA1 hashing | Emails stored as SHA1 hashes |
| 🗃️ Deduplication | Built-in duplicate detection |
| 📦 JSONL output | Chunked output, BreachScope-compatible |

## Quick Start

```bash
git clone https://github.com/Youssefzdb/breachscope-extractor
cd breachscope-extractor
pip install aiohttp tqdm
python3 extractor.py
```

## Usage

```bash
# Start / auto-resume
python3 extractor.py

# Max speed (more threads)
python3 extractor.py --threads 128

# Custom output directory
python3 extractor.py --output /data/breach_output

# Start fresh (ignore saved progress)
python3 extractor.py --reset
```

## Options

```
--threads N      Concurrent HTTP connections (default: 64)
--output DIR     Output directory for JSONL chunks (default: output/)
--chunk-size N   Records per output file (default: 500,000)
--reset          Ignore saved progress and start fresh
```

## Output Format (JSONL)

```json
{"e": "user@example.com", "p": "password123", "eh": "sha1_of_email", "d": "example.com"}
```

## Auto-Resume

Progress is saved automatically to `progress.json` after every batch.
Safe to `Ctrl+C` at any time — just restart to continue:

```bash
python3 extractor.py   # resumes automatically
```

## Dataset

- **Source:** Internet Archive — Collection #1-5 & Antipublic tri-pack
- **Format:** SquashFS (xz-compressed), ~280GB
- **Estimated records:** 3–7 billion unique credentials

---

Built by [Youssef Zaidi](https://github.com/Youssefzdb) — Cybersecurity Specialist

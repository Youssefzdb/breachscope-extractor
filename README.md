# BreachScope Extractor 🔍

A high-performance tool to extract credentials from the **280GB Collection #1-5 & Antipublic** SquashFS archive hosted on the Internet Archive, using HTTP Range requests — no full download required.

## Features

- ⚡ **No 280GB download needed** — uses HTTP Range requests to extract files on-the-fly
- 🔄 **Auto-resume** — survives crashes and restarts, picks up where it left off
- 🧵 **Multi-threaded** — configurable parallel workers for maximum speed
- 📊 **Progress tracking** — real-time stats with ETA
- 🗃️ **JSONL output** — normalized format compatible with BreachScope ingestion
- 🔒 **SHA1 hashing** — passwords stored as SHA1 hashes
- 📦 **Deduplication** — built-in duplicate detection
- 🐙 **GitHub upload** — automatic chunked upload to GitHub repository

## Dataset Info

| Property | Value |
|----------|-------|
| Source | Internet Archive |
| Format | SquashFS (xz-compressed) |
| Size | ~280GB |
| Contents | Collection #1-5, Antipublic, bigDB |
| Estimated records | 3–7 billion |

## Requirements

```bash
pip install requests tqdm
# Optional: git (for GitHub upload)
```

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/Youssefzdb/breachscope-extractor
cd breachscope-extractor

# 2. Install deps
pip install -r requirements.txt

# 3. Run the extractor
python3 extractor.py

# 4. Run with custom options
python3 extractor.py --threads 16 --output ./output --chunk-size 100000

# 5. Upload results to GitHub
python3 github_uploader.py --repo Youssefzdb/breach-data --token YOUR_TOKEN
```

## Output Format (JSONL)

Each line is a JSON object:
```json
{"e": "user@example.com", "p": "password123", "eh": "sha1hash...", "d": "example.com"}
```

- `e` — email address
- `p` — plaintext password (optional, depends on source)
- `eh` — SHA1 hash of email (for privacy-preserving lookup)
- `d` — domain extracted from email

## Architecture

```
extractor.py          # Main extraction engine (HTTP Range)
github_uploader.py    # Chunked GitHub upload tool
progress.json         # Auto-saved progress state
output/               # Extracted JSONL chunks
```

## Progress Tracking

Progress is automatically saved to `progress.json`. To resume:
```bash
python3 extractor.py  # Automatically resumes from last checkpoint
```

## GitHub Upload

Chunks are uploaded as files to a GitHub repository using the GitHub API:
```bash
python3 github_uploader.py \
  --repo Youssefzdb/breach-data \
  --token ghp_YOUR_TOKEN \
  --dir ./output
```

## Legal Notice

This tool is intended for **threat intelligence research and security analysis only**.
Use responsibly and in accordance with applicable laws.

Built by [Youssef Zaidi](https://github.com/Youssefzdb) — Cybersecurity Specialist

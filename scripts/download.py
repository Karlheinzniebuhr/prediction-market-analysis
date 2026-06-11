"""
Cross-platform data downloader for prediction-market-analysis.

Downloads the compressed data archive from S3, decompresses (zstd), and extracts.
Works on Windows, macOS, and Linux without external tool dependencies.

Usage:
  python scripts/download.py
  python scripts/download.py --output-dir data
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import tarfile
from pathlib import Path

URL = "https://s3.jbecker.dev/data.tar.zst"
SENTINEL = ".download_complete"


def download_file(url: str, dest: Path, *, max_attempts: int = 8) -> None:
    """Download a file with progress bar, resume, and retry using httpx."""
    try:
        import httpx
    except ImportError:
        print("ERROR: httpx is required. Install with: pip install httpx", file=sys.stderr)
        sys.exit(1)

    print(f"Downloading {url} ...")
    dest.parent.mkdir(parents=True, exist_ok=True)

    timeout = httpx.Timeout(connect=60.0, read=300.0, write=60.0, pool=60.0)
    for attempt in range(1, max_attempts + 1):
        resume_from = dest.stat().st_size if dest.exists() else 0
        headers = {}
        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"
            print(f"Resuming from {resume_from / 1024 / 1024:.1f} MB (attempt {attempt}/{max_attempts})")

        try:
            with httpx.stream(
                "GET",
                url,
                follow_redirects=True,
                timeout=timeout,
                headers=headers,
            ) as resp:
                if resume_from > 0 and resp.status_code == 416:
                    print("Partial file already complete; continuing.")
                    break
                if resume_from > 0 and resp.status_code not in (206, 200):
                    resp.raise_for_status()
                elif resume_from == 0:
                    resp.raise_for_status()

                if resume_from > 0 and resp.status_code == 200:
                    raise httpx.HTTPError("Server ignored Range request; restart without resume.")

                content_range = resp.headers.get("content-range", "")
                if content_range:
                    total = int(content_range.split("/")[-1])
                else:
                    total = int(resp.headers.get("content-length", 0)) + resume_from

                downloaded = resume_from
                mode = "ab" if resume_from > 0 else "wb"
                with open(dest, mode) as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded / total * 100
                            mb = downloaded / 1024 / 1024
                            total_mb = total / 1024 / 1024
                            print(
                                f"\r  {mb:.1f} / {total_mb:.1f} MB ({pct:.1f}%)",
                                end="",
                                flush=True,
                            )
                print()
            print(f"Downloaded to {dest}")
            return
        except (httpx.HTTPError, OSError) as exc:
            if attempt >= max_attempts:
                raise
            if "restart without resume" in str(exc):
                dest.unlink(missing_ok=True)
            wait_s = min(60, 2 ** attempt)
            print(f"\nDownload interrupted: {exc}")
            print(f"Retrying in {wait_s}s ...")
            import time

            time.sleep(wait_s)


def decompress_and_extract(archive_path: Path, output_dir: Path) -> None:
    """Decompress zstd archive and extract tar contents."""
    try:
        import zstandard
    except ImportError:
        print(
            "ERROR: zstandard is required for decompression.\n"
            "  Install with: pip install zstandard",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Decompressing and extracting {archive_path.name} ...")
    dctx = zstandard.ZstdDecompressor()

    with open(archive_path, "rb") as compressed:
        reader = dctx.stream_reader(compressed)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            tar.extractall(path=str(output_dir), filter="data")

    print("Extraction complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download prediction-market-analysis dataset")
    parser.add_argument("--output-dir", default="data", help="Output directory (default: data)")
    parser.add_argument("--force", action="store_true", help="Force re-download even if already complete")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    sentinel = output_dir / SENTINEL
    archive_path = output_dir / "data.tar.zst"

    if sentinel.exists() and not args.force:
        print("Data already downloaded and extracted. Use --force to re-download.")
        return

    download_file(URL, archive_path)
    decompress_and_extract(archive_path, Path("."))

    if archive_path.exists():
        print("Cleaning up archive ...")
        archive_path.unlink()

    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.touch()
    print("Data directory ready.")


if __name__ == "__main__":
    main()

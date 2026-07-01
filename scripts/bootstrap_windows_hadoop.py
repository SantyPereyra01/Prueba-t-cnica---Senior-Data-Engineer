"""Install checksum-pinned Hadoop compatibility binaries for local Windows Spark."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / ".hadoop" / "bin"
RAW_ROOT = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
ARTIFACTS = {
    "winutils.exe": (
        f"{RAW_ROOT}/winutils.exe",
        "496a591eb1e67df2a620f710d529ba6ddfe1c19149e6647cc4e320bb0efd8553",
    ),
    "hadoop.dll": (
        f"{RAW_ROOT}/hadoop.dll",
        "d7ab36a68518748cef142be2da5069b4c763c2cd764c1d2e6ac48c7200405be3",
    ),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    if os.name != "nt":
        print("Windows Hadoop compatibility binaries are not required on this platform.")
        return
    TARGET.mkdir(parents=True, exist_ok=True)
    for filename, (url, expected_hash) in ARTIFACTS.items():
        destination = TARGET / filename
        if destination.is_file() and _sha256(destination) == expected_hash:
            continue
        partial = destination.with_suffix(destination.suffix + ".part")
        with urlopen(url, timeout=60) as response, partial.open("wb") as output:  # noqa: S310
            output.write(response.read())
        actual_hash = _sha256(partial)
        if actual_hash != expected_hash:
            partial.unlink(missing_ok=True)
            raise RuntimeError(f"Checksum mismatch for {filename}: {actual_hash}")
        partial.replace(destination)
        print(f"Downloaded and verified {filename}")


if __name__ == "__main__":
    main()

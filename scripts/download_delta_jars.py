"""Download pinned Delta runtime JARs for winutils-free local Windows runs."""

from __future__ import annotations

from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / ".spark-jars"
MAVEN_ROOT = "https://repo1.maven.org/maven2"
ARTIFACTS = {
    "io.delta_delta-spark_2.12-3.2.0.jar": (
        f"{MAVEN_ROOT}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar"
    ),
    "io.delta_delta-storage-3.2.0.jar": (
        f"{MAVEN_ROOT}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"
    ),
    "org.antlr_antlr4-runtime-4.9.3.jar": (
        f"{MAVEN_ROOT}/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar"
    ),
}


def main() -> None:
    TARGET.mkdir(exist_ok=True)
    for filename, url in ARTIFACTS.items():
        destination = TARGET / filename
        if destination.is_file() and destination.stat().st_size > 0:
            continue
        with urlopen(url, timeout=60) as response, destination.open("wb") as output:  # noqa: S310
            output.write(response.read())
        print(f"Downloaded {filename}")


if __name__ == "__main__":
    main()

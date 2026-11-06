import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def write_json(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2, default=str, )
        file.write("\n")


def write_ndjson(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False, default=str, ) + "\n")


def calculate_sha256(path: Path) -> str:
    hasher = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b"", ):
            hasher.update(chunk)

    return hasher.hexdigest()


def save_trade_data(trade_data: dict, summary: dict, output_dir: str | Path = "output", ) -> dict:
    # 依照月份建立輸出資料夾
    period = summary["period"]
    destination = Path(output_dir) / period
    destination.mkdir(parents=True, exist_ok=True)

    records_path = destination / "trade_records.ndjson"
    summary_path = destination / "summary.json"
    manifest_path = destination / "manifest.json"

    records = (trade_data["world"] + trade_data["partners"])

    write_ndjson(records_path, records)
    write_json(summary_path, summary)

    # 建立 Manifest，記錄檔案完整性資訊
    manifest = {
        "period": period,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "world_count": len(trade_data["world"]),
        "partner_count": len(trade_data["partners"]),
        "files": {
            "trade_records.ndjson": {"sha256": calculate_sha256(records_path), },
            "summary.json": {"sha256": calculate_sha256(summary_path), },
        },
    }

    write_json(manifest_path, manifest)

    return manifest

"""Komponen antarmuka terminal: form interaktif dan notifikasi.

Form dipakai ketika program dijalankan tanpa argumen di terminal (TTY) atau
dengan `--interactive`. Di Docker tanpa `-it`, stdin bukan TTY sehingga form
otomatis dilewati dan nilai default/argumen tetap dipakai.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass


def timestamp() -> str:
    return time.strftime("%H:%M:%S")


def notify(tag: str, message: str) -> None:
    print(f"[{timestamp()}] [{tag}] {message}", flush=True)


def should_prompt(requested: bool, raw_args) -> bool:
    """Tampilkan form hanya di terminal interaktif."""
    if not sys.stdin.isatty():
        return False
    return bool(requested) or len(raw_args) == 0


@dataclass
class Field:
    key: str
    label: str
    value: object
    kind: str = "str"  # str | int | float | choice | bool
    choices: tuple = ()
    minimum: float | None = None


def format_value(value, field: Field) -> str:
    if field.kind == "bool":
        return "ya" if value else "tidak"
    if value is None or value == "":
        return "(kosong)"
    return str(value)


def parse_value(field: Field, text: str):
    if field.kind == "choice":
        lowered = text.lower()
        for choice in field.choices:
            if choice.lower() == lowered or choice.lower().startswith(lowered):
                return choice
        raise ValueError(f"pilihan yang tersedia: {', '.join(field.choices)}")

    if field.kind == "int":
        try:
            value = int(text)
        except ValueError as exc:
            raise ValueError("harus bilangan bulat") from exc
        if field.minimum is not None and value < field.minimum:
            raise ValueError(f"minimal {int(field.minimum)}")
        return value

    if field.kind == "float":
        try:
            value = float(text.replace(",", "."))
        except ValueError as exc:
            raise ValueError("harus angka") from exc
        if field.minimum is not None and value < field.minimum:
            raise ValueError(f"minimal {field.minimum}")
        return value

    if field.kind == "bool":
        lowered = text.lower()
        if lowered in ("y", "ya", "t", "true", "1"):
            return True
        if lowered in ("n", "tidak", "false", "0"):
            return False
        raise ValueError("isi 'y' atau 'n'")

    return text


def run_form(title: str, fields, subtitle: str = "") -> dict:
    """Tampilkan form menu; kembalikan dict nilai terbaru."""
    values = {field.key: field.value for field in fields}

    print()
    print("=" * 64)
    print(f" {title}")
    if subtitle:
        print(f" {subtitle}")
    print("=" * 64)

    while True:
        for index, field in enumerate(fields, 1):
            print(
                f"  [{index}] {field.label:<26}: "
                f"{format_value(values[field.key], field)}"
            )
        print("  [0] Mulai")
        try:
            raw = input("Ubah nomor (Enter = mulai): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return values

        if raw in ("", "0"):
            return values

        if not raw.isdigit() or not (1 <= int(raw) <= len(fields)):
            print("  ! nomor tidak valid")
            continue

        field = fields[int(raw) - 1]
        try:
            text = input(
                f"  {field.label} [{format_value(values[field.key], field)}]: "
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return values

        if not text:
            continue
        try:
            values[field.key] = parse_value(field, text)
        except ValueError as exc:
            print(f"  ! {exc}")

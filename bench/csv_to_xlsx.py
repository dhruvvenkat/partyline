#!/usr/bin/env python3
"""Combine CSV files into one XLSX workbook, one worksheet per file."""

import argparse
import csv
import re
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr


NUMBER = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?\Z")
INVALID_SHEET_CHARS = re.compile(r"[\\/*?:\[\]]")
INVALID_XML_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
XML_HEADER = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
SHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
OFFICE_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def column_name(number):
    name = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def sheet_names(paths):
    names = []
    used = set()
    for path in paths:
        base = INVALID_SHEET_CHARS.sub("_", path.stem).strip("'") or "Sheet"
        name = base[:31]
        suffix = 2
        while name.casefold() in used:
            ending = f"_{suffix}"
            name = base[: 31 - len(ending)] + ending
            suffix += 1
        used.add(name.casefold())
        names.append(name)
    return names


def cell_xml(reference, value):
    value = INVALID_XML_CHARS.sub("\ufffd", value)
    if NUMBER.fullmatch(value):
        return f'<c r="{reference}"><v>{value}</v></c>'
    preserve = ' xml:space="preserve"' if value[:1].isspace() or value[-1:].isspace() else ""
    return f'<c r="{reference}" t="inlineStr"><is><t{preserve}>{escape(value)}</t></is></c>'


def worksheet_xml(path):
    rows = []
    with path.open(encoding="utf-8-sig", newline="") as source:
        for row_number, row in enumerate(csv.reader(source, strict=True), 1):
            if row_number > 1_048_576:
                raise ValueError(f"{path}: exceeds Excel's row limit")
            if len(row) > 16_384:
                raise ValueError(f"{path}: row {row_number} exceeds Excel's column limit")
            cells = "".join(
                cell_xml(f"{column_name(column)}{row_number}", value)
                for column, value in enumerate(row, 1)
                if value != ""
            )
            rows.append(f'<row r="{row_number}">{cells}</row>')
    return f'{XML_HEADER}<worksheet xmlns="{SHEET_NS}"><sheetData>{"".join(rows)}</sheetData></worksheet>'


def create_workbook(output, csv_paths):
    names = sheet_names(csv_paths)
    sheet_entries = "".join(
        f'<sheet name={quoteattr(name)} sheetId="{index}" r:id="rId{index}"/>'
        for index, name in enumerate(names, 1)
    )
    sheet_relationships = "".join(
        f'<Relationship Id="rId{index}" Type="{OFFICE_REL_NS}/worksheet" '
        f'Target="worksheets/sheet{index}.xml"/>'
        for index in range(1, len(csv_paths) + 1)
    )
    sheet_types = "".join(
        f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for index in range(1, len(csv_paths) + 1)
    )

    try:
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as workbook:
            workbook.writestr(
                "[Content_Types].xml",
                f'{XML_HEADER}<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                f"{sheet_types}</Types>",
            )
            workbook.writestr(
                "_rels/.rels",
                f'{XML_HEADER}<Relationships xmlns="{REL_NS}">'
                f'<Relationship Id="rId1" Type="{OFFICE_REL_NS}/officeDocument" '
                'Target="xl/workbook.xml"/></Relationships>',
            )
            workbook.writestr(
                "xl/workbook.xml",
                f'{XML_HEADER}<workbook xmlns="{SHEET_NS}" xmlns:r="{OFFICE_REL_NS}">'
                f"<sheets>{sheet_entries}</sheets></workbook>",
            )
            workbook.writestr(
                "xl/_rels/workbook.xml.rels",
                f'{XML_HEADER}<Relationships xmlns="{REL_NS}">{sheet_relationships}</Relationships>',
            )
            for index, path in enumerate(csv_paths, 1):
                workbook.writestr(f"xl/worksheets/sheet{index}.xml", worksheet_xml(path))
    except Exception:
        output.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="output .xlsx file")
    parser.add_argument("csv", nargs="+", type=Path, help="input CSV files")
    args = parser.parse_args()

    if args.output.suffix.lower() != ".xlsx":
        parser.error("output filename must end in .xlsx")
    if args.output.exists():
        parser.error(f"output already exists: {args.output}")
    missing = [str(path) for path in args.csv if not path.is_file()]
    if missing:
        parser.error(f"input file not found: {', '.join(missing)}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        create_workbook(args.output, args.csv)
    except (OSError, UnicodeError, csv.Error, ValueError) as error:
        parser.error(str(error))
    print(f"wrote {args.output} with {len(args.csv)} worksheets")


if __name__ == "__main__":
    main()

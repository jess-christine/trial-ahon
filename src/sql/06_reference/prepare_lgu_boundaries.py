"""Prepare the supplied GeoParquet for the existing boundary loader; never fuzzy-match."""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import openpyxl


def map_code(source_code, source_name, references):
    name = str(source_name or "").strip().casefold()
    code = str(source_code or "")
    candidates = []
    if re.fullmatch(r"PH[0-9]{7}", code):
        digits = code[2:]
        candidates = [r for r in references if r["psgc_code"] == digits + "000"
                      or r["correspondence_code"] == digits[:2] + digits[3:] + "000"]
    else:
        candidates = [r for r in references if r["psgc_code"] == code]
    matches = {r["psgc_code"]: r for r in candidates if r["lgu_name"].strip().casefold() == name}
    method = "code_and_exact_name"
    if not matches:
        matches = {r["psgc_code"]: r for r in references if r["lgu_name"].strip().casefold() == name}
        method = "unique_exact_name"
    if len(matches) != 1:
        return None, "ambiguous" if matches else "unmatched", None
    return next(iter(matches.values())), "matched", method


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", required=True, type=Path)
    parser.add_argument("--psgc-workbook", required=True, type=Path)
    parser.add_argument("--geojson", required=True, type=Path)
    parser.add_argument("--crosswalk", required=True, type=Path)
    args = parser.parse_args()
    # These are local preparation tools, not additional job dependencies.
    import pyarrow.parquet as pq
    from shapely import from_wkb
    from shapely.geometry import mapping

    table = pq.read_table(args.parquet)
    geo = json.loads((table.schema.metadata or {}).get(b"geo", b"{}"))
    geometry_metadata = geo.get("columns", {}).get("geometry", {})
    if geometry_metadata.get("encoding") != "WKB" or geometry_metadata.get("crs", {}).get("id") != {"authority": "EPSG", "code": 4326}:
        raise ValueError("Expected GeoParquet WKB geometry explicitly declared EPSG:4326")
    required = {"canonical_psgc_code", "canonical_lgu_name", "geometry"}
    if not required.issubset(table.column_names):
        raise ValueError(f"Missing boundary columns: {sorted(required - set(table.column_names))}")
    workbook = openpyxl.load_workbook(args.psgc_workbook, read_only=True, data_only=True)
    rows = workbook["PSGC"].iter_rows(values_only=True)
    headers = next(rows)
    indexes = {name: headers.index(name) for name in ("10-digit PSGC", "Name", "Correspondence Code", "Geographic Level")}
    references = [{"psgc_code": str(r[indexes["10-digit PSGC"]]), "lgu_name": str(r[indexes["Name"]]),
                   "correspondence_code": str(r[indexes["Correspondence Code"]]).zfill(9)}
                  for r in rows if r[indexes["Geographic Level"]] in ("City", "Mun")]
    workbook.close()
    if not len(table):
        raise ValueError("Boundary input has no rows")
    features, crosswalk = [], []
    counts = defaultdict(int)
    for row in table.to_pylist():
        match, status, method = map_code(row["canonical_psgc_code"], row["canonical_lgu_name"], references)
        counts[status] += 1
        geometry = from_wkb(row["geometry"])
        if geometry.is_empty or not geometry.is_valid or geometry.geom_type not in ("Polygon", "MultiPolygon"):
            raise ValueError(f"Invalid boundary geometry: {row['canonical_psgc_code']}")
        properties = {k: v for k, v in row.items() if k != "geometry"}
        properties.update(geometry_wkb_hex=row["geometry"].hex(), psgc_code=match["psgc_code"] if match else None,
                          mapping_status=status, mapping_method=method, mapping_reference=args.psgc_workbook.name)
        features.append({"type": "Feature", "properties": properties, "geometry": mapping(geometry)})
        crosswalk.append({"source_code": row["canonical_psgc_code"], "source_lgu_name": row["canonical_lgu_name"],
                          "psgc_code": match["psgc_code"] if match else "", "match_status": status,
                          "method": method or "", "reference_file": args.psgc_workbook.name})
    assigned = [row["psgc_code"] for row in crosswalk if row["psgc_code"]]
    if len(assigned) != len(set(assigned)):
        raise ValueError("Multiple polygons map to the same PSGC; review the crosswalk before publishing")
    for path in (args.geojson, args.crosswalk):
        if path.exists():
            raise FileExistsError(f"Preserve existing output: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
    args.geojson.write_text(json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False), encoding="utf-8")
    with args.crosswalk.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(crosswalk[0]))
        writer.writeheader()
        writer.writerows(crosswalk)
    print(json.dumps({"rows": len(features), "mapping_counts": dict(counts)}, sort_keys=True))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def sha(path):
 h=hashlib.sha256()
 with path.open("rb") as f:
  for b in iter(lambda:f.read(1<<20),b""): h.update(b)
 return h.hexdigest()
files=[]
for p in sorted(ROOT.rglob("*")):
 if p.is_file() and p.name not in {"MANIFEST.json", ".DS_Store"} and ".git" not in p.parts and "__pycache__" not in p.parts:
  files.append({"path":str(p.relative_to(ROOT)),"bytes":p.stat().st_size,"sha256":sha(p)})
manifest={"schema":"smartdata-public-repository/v1","generated_at":"2026-09-23","measured_harness_tag":"stand-final-v1.6","measured_harness_commit":"d965b352d3708d6d712b0e4142fc0ea7c2b79552","published_cells":912,"source_result_sha256":sha(ROOT/"results"/"measurements.csv"),"files":files}
(ROOT/"MANIFEST.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")

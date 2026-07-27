"""Provider-free clean_cutover_v1 snapshot boundary."""
from __future__ import annotations
import hashlib, json, math, os, tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence
SCHEMA_VERSION="soxl_tqqq_clean_cutover_snapshot.v1"; EVIDENCE_GENERATION="clean_cutover_v1"; PAIR_ID="QQQ_TQQQ"; SYMBOLS=("QQQ","TQQQ"); PLUGIN_STATE="ABSENT_DISABLED"
class SnapshotValidationError(ValueError): pass
def _digest(value:Any)->str:
 data=value if isinstance(value,(bytes,bytearray)) else json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode(); return hashlib.sha256(data).hexdigest()
def quarantine_raw_payload(payload:Mapping[str,Any],retrieval_receipt:Mapping[str,Any])->dict[str,Any]:
 if not isinstance(payload,Mapping) or not isinstance(retrieval_receipt,Mapping): raise SnapshotValidationError("payload and retrieval_receipt must be mappings")
 return {"raw_payload":dict(payload),"retrieval_receipt":dict(retrieval_receipt),"offline_fixture":True}
def _validate_rows(rows:Sequence[Mapping[str,Any]],sessions:Sequence[str])->list[dict[str,Any]]:
 if not rows: raise SnapshotValidationError("rows must not be empty")
 grouped={}; expected=set(sessions)
 for row in rows:
  if set(row)!={"session","symbol","adjusted_close"}: raise SnapshotValidationError("invalid row shape")
  session,symbol,value=row["session"],row["symbol"],row["adjusted_close"]
  if not isinstance(session,str) or len(session)!=10 or session[4]!="-" or session[7]!="-": raise SnapshotValidationError("invalid session")
  if symbol not in SYMBOLS or isinstance(value,bool) or not isinstance(value,(int,float,str)): raise SnapshotValidationError("invalid row")
  try: number=float(value)
  except (ValueError,TypeError,OverflowError) as exc: raise SnapshotValidationError("invalid adjusted_close") from exc
  if not math.isfinite(number) or number<=0: raise SnapshotValidationError("invalid adjusted_close")
  grouped.setdefault(session,[]).append(row)
 if set(grouped)!=expected or any(len(v)!=2 or {r["symbol"] for r in v}!=set(SYMBOLS) for v in grouped.values()): raise SnapshotValidationError("each session must contain exactly QQQ and TQQQ")
 out=[dict(r) for s in sorted(grouped) for r in sorted(grouped[s],key=lambda r:r["symbol"])]
 if list(rows)!=out: raise SnapshotValidationError("rows are not canonical")
 return out
def materialize_clean_cutover_snapshot(*,destination:Path,rows:Sequence[Mapping[str,Any]],sessions:Sequence[str],source_sha256:str,calendar_sha256:str,external_manifest_sha256:str,timezone:str="UTC",adjusted_price_semantics:str="adjusted_close")->dict[str,Any]:
 destination=Path(destination)
 if destination.exists(): raise SnapshotValidationError("destination exists (no-clobber)")
 canonical=_validate_rows(rows,sessions); identity=_digest({"schema":SCHEMA_VERSION,"generation":EVIDENCE_GENERATION,"pair":PAIR_ID,"source":source_sha256,"calendar":calendar_sha256,"external":external_manifest_sha256,"sessions":list(sessions)})
 artifact={"schema_version":SCHEMA_VERSION,"evidence_generation":EVIDENCE_GENERATION,"pair_id":PAIR_ID,"plugin_state":PLUGIN_STATE,"size_zero":True,"source_sha256":source_sha256,"calendar_sha256":calendar_sha256,"external_manifest_sha256":external_manifest_sha256,"timezone":timezone,"adjusted_price_semantics":adjusted_price_semantics,"sessions":list(sessions),"rows":canonical,"snapshot_identity":identity,"offline_fixture":True}
 destination.parent.mkdir(parents=True,exist_ok=True); fd,tmp=tempfile.mkstemp(prefix=f".{destination.name}.",dir=destination.parent)
 try:
  with os.fdopen(fd,"w",encoding="utf-8") as h: json.dump(artifact,h,sort_keys=True,separators=(",",":")); h.write("\n"); h.flush(); os.fsync(h.fileno())
  os.link(tmp,destination)
 except FileExistsError as exc: raise SnapshotValidationError("destination exists (no-clobber)") from exc
 finally:
  try: os.unlink(tmp)
  except FileNotFoundError: pass
 return artifact
def strict_readback_clean_cutover_snapshot(*,path:Path,expected_source_sha256:str,expected_calendar_sha256:str,expected_manifest_sha256:str)->dict[str,Any]:
 path=Path(path)
 if not path.is_file() or path.is_symlink(): raise SnapshotValidationError("snapshot must be regular non-symlink file")
 flags=getattr(os,"O_NOFOLLOW",0)
 with path.open("r",encoding="utf-8",opener=lambda p,f:os.open(p,f|flags)) as h: artifact=json.load(h)
 if (artifact.get("source_sha256"),artifact.get("calendar_sha256"),artifact.get("external_manifest_sha256"))!=(expected_source_sha256,expected_calendar_sha256,expected_manifest_sha256): raise SnapshotValidationError("digest binding mismatch")
 _validate_rows(artifact.get("rows",[]),artifact.get("sessions",[]))
 if artifact.get("evidence_generation")!=EVIDENCE_GENERATION or artifact.get("pair_id")!=PAIR_ID or artifact.get("plugin_state")!=PLUGIN_STATE or artifact.get("size_zero") is not True: raise SnapshotValidationError("identity mismatch")
 return artifact

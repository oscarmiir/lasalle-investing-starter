"""Lake en Cloudflare R2 (API compatible con S3): subida de raw, listado y recuperación verificada.

Credenciales en .env (ver .env.example; Git lo ignora):
    uv run --group cloud --env-file .env python lake_cloud.py subir        # lake/raw -> s3://<bucket>/raw/
    uv run --group cloud --env-file .env python lake_cloud.py listar [--prefijo lake/]
    uv run --group cloud --env-file .env python lake_cloud.py recuperar --destino <carpeta>

Estructura en el bucket:  raw/<fuente>/<descarga>/<TICKER>.json + manifest.json  (originales)
                          lake/clean, lake/curated, lake/quarantine             (Parquet que escribe el ETL)
La subida no sobrescribe un original existente: si el objeto ya está, compara su huella SHA-256.
La recuperación descarga raw y verifica cada fichero con la huella de su manifiesto.
"""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from common import ROOT


def client():
    missing = [name for name in ("R2_ENDPOINT", "R2_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not os.environ.get(name)]
    if missing:
        sys.exit(f"Faltan variables de entorno: {', '.join(missing)}. Usa --env-file .env (ver .env.example).")
    return boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT"], region_name="auto"), os.environ["R2_BUCKET"]


def sha256(body):
    return hashlib.sha256(body).hexdigest()


def upload_raw(s3, bucket, raw=ROOT / "lake/raw"):
    uploaded = skipped = 0
    for path in sorted(p for p in raw.rglob("*") if p.is_file()):
        key = "raw/" + path.relative_to(raw).as_posix()
        body = path.read_bytes()
        try:
            remote = s3.head_object(Bucket=bucket, Key=key)["Metadata"].get("sha256")
            if remote != sha256(body):
                sys.exit(f"{key} ya existe en el bucket con otra huella: raw no se sobrescribe.")
            skipped += 1
            continue
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
        s3.put_object(Bucket=bucket, Key=key, Body=body, Metadata={"sha256": sha256(body)})
        uploaded += 1
    print(f"raw -> s3://{bucket}/raw/: {uploaded} subidos, {skipped} ya presentes con la misma huella")


def objects(s3, bucket, prefix):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def list_objects(s3, bucket, prefix):
    items = list(objects(s3, bucket, prefix))
    for item in items:
        print(f"{item['LastModified']:%Y-%m-%d %H:%M}  {item['Size']:>9}  {item['Key']}")
    print(f"{len(items)} objetos, {sum(i['Size'] for i in items) / 1e6:.2f} MB bajo s3://{bucket}/{prefix}")


def recover(s3, bucket, destination):
    destination = Path(destination)
    keys = [item["Key"] for item in objects(s3, bucket, "raw/")]
    for key in keys:
        target = destination / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    checked, errors = 0, []
    for manifest in sorted(destination.glob("raw/*/*/manifest.json")):
        for entry in json.loads(manifest.read_text(encoding="utf-8"))["files"]:
            path = manifest.parent / entry["file"]
            if not path.exists() or sha256(path.read_bytes()) != entry["sha256"]:
                errors.append(str(path))
            checked += 1
    print(f"Recuperados {len(keys)} objetos en {destination}; {checked} originales verificados con su manifiesto; "
          f"{len(errors)} errores")
    if errors:
        sys.exit("Huellas que no coinciden: " + ", ".join(errors))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("accion", choices=("subir", "listar", "recuperar"))
    parser.add_argument("--prefijo", default="")
    parser.add_argument("--destino", type=Path)
    args = parser.parse_args()
    s3, bucket = client()
    if args.accion == "subir":
        upload_raw(s3, bucket)
    elif args.accion == "listar":
        list_objects(s3, bucket, args.prefijo)
    else:
        if not args.destino:
            parser.error("recuperar necesita --destino")
        recover(s3, bucket, args.destino)

from __future__ import annotations

import argparse
import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlretrieve

SUPPORTED_TABLE_SUFFIXES = frozenset({".csv", ".json", ".jsonl", ".parquet"})
SUPPORTED_CONFIG_SUFFIXES = frozenset({".json"})

CopyFn = Callable[[str, Path], None]


@dataclass(frozen=True)
class ResolvedInputSources:
    prices_path: Path
    universe_path: Path
    config_path: Path | None = None
    product_map_path: Path | None = None


def is_gcs_uri(source: str) -> bool:
    return str(source or "").strip().startswith("gs://")


def is_http_uri(source: str) -> bool:
    normalized = str(source or "").strip().lower()
    return normalized.startswith("https://") or normalized.startswith("http://")


def source_needs_gcloud(source: str | None) -> bool:
    return is_gcs_uri(str(source or ""))


def _default_gcs_copy(source: str, target: Path) -> None:
    subprocess.run(["gcloud", "storage", "cp", source, str(target)], check=True)


def _default_http_copy(source: str, target: Path) -> None:
    urlretrieve(source, target)  # noqa: S310 - operator-supplied data source URL.


def _source_suffix(source: str, *, allowed_suffixes: frozenset[str], default_suffix: str) -> str:
    parsed = urlparse(str(source).strip())
    candidate_path = parsed.path or str(source).strip()
    suffix = Path(candidate_path).suffix.lower()
    return suffix if suffix in allowed_suffixes else default_suffix


def resolve_input_source(
    source: str | Path,
    *,
    output_dir: str | Path,
    stem: str,
    allowed_suffixes: frozenset[str] = SUPPORTED_TABLE_SUFFIXES,
    default_suffix: str = ".csv",
    gcs_copy: CopyFn | None = None,
    http_copy: CopyFn | None = None,
) -> Path:
    source_text = str(source or "").strip()
    if not source_text:
        raise EnvironmentError(f"{stem} source is required")

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    if is_gcs_uri(source_text):
        target = output_root / f"{stem}{_source_suffix(source_text, allowed_suffixes=allowed_suffixes, default_suffix=default_suffix)}"
        (gcs_copy or _default_gcs_copy)(source_text, target)
        if not target.exists():
            raise FileNotFoundError(f"resolved GCS input was not created: {target}")
        return target

    if is_http_uri(source_text):
        target = output_root / f"{stem}{_source_suffix(source_text, allowed_suffixes=allowed_suffixes, default_suffix=default_suffix)}"
        (http_copy or _default_http_copy)(source_text, target)
        if not target.exists():
            raise FileNotFoundError(f"resolved HTTP input was not created: {target}")
        return target

    local_path = Path(source_text).expanduser()
    if not local_path.exists():
        raise FileNotFoundError(f"input file not found: {local_path}")
    if not local_path.is_file():
        raise FileNotFoundError(f"input path is not a file: {local_path}")
    suffix = local_path.suffix.lower()
    if suffix not in allowed_suffixes:
        expected = ", ".join(sorted(allowed_suffixes))
        raise ValueError(f"unsupported {stem} file suffix {suffix!r}; expected one of: {expected}")
    return local_path


def resolve_input_sources(
    *,
    prices_source: str | Path,
    universe_source: str | Path,
    output_dir: str | Path,
    config_source: str | Path | None = None,
    product_map_source: str | Path | None = None,
    gcs_copy: CopyFn | None = None,
    http_copy: CopyFn | None = None,
) -> ResolvedInputSources:
    prices_path = resolve_input_source(
        prices_source,
        output_dir=output_dir,
        stem="prices",
        gcs_copy=gcs_copy,
        http_copy=http_copy,
    )
    universe_path = resolve_input_source(
        universe_source,
        output_dir=output_dir,
        stem="universe",
        gcs_copy=gcs_copy,
        http_copy=http_copy,
    )
    config_path = None
    if str(config_source or "").strip():
        config_path = resolve_input_source(
            str(config_source),
            output_dir=output_dir,
            stem="config",
            allowed_suffixes=SUPPORTED_CONFIG_SUFFIXES,
            default_suffix=".json",
            gcs_copy=gcs_copy,
            http_copy=http_copy,
        )
    product_map_path = None
    if str(product_map_source or "").strip():
        product_map_path = resolve_input_source(
            str(product_map_source),
            output_dir=output_dir,
            stem="product_map",
            gcs_copy=gcs_copy,
            http_copy=http_copy,
        )
    return ResolvedInputSources(
        prices_path=prices_path,
        universe_path=universe_path,
        config_path=config_path,
        product_map_path=product_map_path,
    )


def _write_shell_env(path: str | Path, resolved: ResolvedInputSources) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"PRICES_PATH={shlex.quote(str(resolved.prices_path))}",
        f"UNIVERSE_PATH={shlex.quote(str(resolved.universe_path))}",
    ]
    if resolved.config_path is not None:
        lines.append(f"CONFIG_PATH={shlex.quote(str(resolved.config_path))}")
    if resolved.product_map_path is not None:
        lines.append(f"PRODUCT_MAP_PATH={shlex.quote(str(resolved.product_map_path))}")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve snapshot input data sources into local files.")
    parser.add_argument("--prices", required=True, help="Local, gs://, or http(s) price history source")
    parser.add_argument("--universe", required=True, help="Local, gs://, or http(s) universe source")
    parser.add_argument("--config", help="Optional local, gs://, or http(s) JSON config source")
    parser.add_argument("--product-map", help="Optional local, gs://, or http(s) product map table source")
    parser.add_argument("--output-dir", required=True, help="Directory for downloaded remote inputs")
    parser.add_argument("--env-output", help="Optional shell env file to write resolved PRICES_PATH/UNIVERSE_PATH/CONFIG_PATH")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    resolved = resolve_input_sources(
        prices_source=args.prices,
        universe_source=args.universe,
        config_source=args.config,
        product_map_source=args.product_map,
        output_dir=args.output_dir,
    )
    print(f"resolved prices -> {resolved.prices_path}")
    print(f"resolved universe -> {resolved.universe_path}")
    if resolved.config_path is not None:
        print(f"resolved config -> {resolved.config_path}")
    if resolved.product_map_path is not None:
        print(f"resolved product map -> {resolved.product_map_path}")
    if args.env_output:
        _write_shell_env(args.env_output, resolved)
        print(f"wrote resolved env -> {args.env_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

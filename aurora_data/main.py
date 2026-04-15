"""Aurora Corp synthetic data generator — CLI orchestrator."""
from __future__ import annotations

import random
import sys
import time
from datetime import date
from pathlib import Path
from typing import Annotated, Optional

import numpy as np
import structlog
import typer
from faker import Faker
from tqdm import tqdm

from .generators import (
    FinanceGenerator,
    HRGenerator,
    ManufacturingGenerator,
    MarketingGenerator,
    MasterDataGenerator,
    ObservabilityGenerator,
    SalesGenerator,
    SocialMediaGenerator,
    SupportGenerator,
    SupplyChainGenerator,
)
from .generators.base import MasterCache, SinkDispatcher, load_profile
from .utils.time_utils import generate_crisis_days

log = structlog.get_logger()

app = typer.Typer(
    name="aurora-data",
    help="Aurora Corp synthetic data generator.",
    add_completion=False,
)

DEFAULT_CONFIG = str(Path(__file__).parent / "config.yaml")

DOMAIN_MAP: dict[str, type] = {
    "master_data": MasterDataGenerator,
    "sales": SalesGenerator,
    "finance": FinanceGenerator,
    "marketing": MarketingGenerator,
    "social_media": SocialMediaGenerator,
    "supply_chain": SupplyChainGenerator,
    "manufacturing": ManufacturingGenerator,
    "hr": HRGenerator,
    "support": SupportGenerator,
    "observability": ObservabilityGenerator,
}

FACT_DOMAINS = [
    "sales",
    "finance",
    "marketing",
    "social_media",
    "supply_chain",
    "manufacturing",
    "hr",
    "support",
    "observability",
]


def _seed_all(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def _resolve_domains(only: str | None) -> list[str]:
    if only is None:
        return FACT_DOMAINS
    requested = [d.strip() for d in only.split(",")]
    unknown = [d for d in requested if d not in DOMAIN_MAP and d != "master_data"]
    if unknown:
        typer.echo(f"Unknown domains: {unknown}. Valid: {list(DOMAIN_MAP.keys())}", err=True)
        raise typer.Exit(1)
    return requested


@app.command()
def generate(
    profile: Annotated[str, typer.Option("--profile", "-p", help="Profile: demo | standard | loadtest")] = "demo",
    seed: Annotated[int, typer.Option("--seed", "-s", help="Random seed for reproducibility")] = 42,
    sink: Annotated[str, typer.Option("--sink", help="Output sink: csv | parquet | postgres | all")] = "parquet",
    only: Annotated[Optional[str], typer.Option("--only", help="Comma-separated list of domains to generate")] = None,
    config: Annotated[str, typer.Option("--config", "-c", help="Path to config.yaml")] = DEFAULT_CONFIG,
) -> None:
    """Generate synthetic Aurora Corp data and write to the selected sink(s)."""
    t_start = time.perf_counter()
    log.info("generation_start", profile=profile, seed=seed, sink=sink)

    # 1. Seed all RNG sources
    _seed_all(seed)

    # 2. Load profile and config
    profile_cfg, full_config = load_profile(config, profile)
    dispatcher = SinkDispatcher.from_flag(sink, profile_cfg, full_config)

    # 3. Phase 1 — Master data
    cache = MasterCache()
    typer.echo(f"[1/3] Generating master data (profile={profile}, seed={seed})")
    master_gen = MasterDataGenerator(seed=seed)
    master_tables = master_gen.generate(cache, profile_cfg, crisis_days=[])
    cache.populate(master_tables)
    dispatcher.write_all(master_tables)
    log.info(
        "master_data_complete",
        tables=list(master_tables.keys()),
        rows={k: len(v) for k, v in master_tables.items()},
    )

    # 4. Pick crisis days
    crisis_days = generate_crisis_days(
        profile_cfg.start,
        profile_cfg.end,
        profile_cfg.crisis_freq_per_month,
        np.random.default_rng(seed + 1),
    )
    log.info("crisis_days", days=[str(d) for d in crisis_days])

    # 5. Phase 2 — Fact domains
    domains = _resolve_domains(only)
    typer.echo(f"[2/3] Generating {len(domains)} fact domain(s): {', '.join(domains)}")

    for domain_name in tqdm(domains, desc="Domains", unit="domain"):
        gen_cls = DOMAIN_MAP[domain_name]
        gen = gen_cls(seed=seed)
        t_domain = time.perf_counter()

        if profile_cfg.name == "loadtest" and hasattr(gen, "generate_chunked"):
            chunk_count = 0
            for chunk in gen.generate_chunked(cache, profile_cfg, crisis_days, profile_cfg.chunk_size):
                dispatcher.write_all(chunk, force_parquet_for_high_vol=True)
                # Populate order cache from sales chunks
                if "orders" in chunk:
                    cache.populate({"orders": chunk["orders"]})
                chunk_count += 1
            log.info("domain_complete_chunked", domain=domain_name, chunks=chunk_count, elapsed=round(time.perf_counter() - t_domain, 2))
        else:
            tables = gen.generate(cache, profile_cfg, crisis_days)
            # Populate FK caches from fact tables
            if "orders" in tables:
                cache.populate({"orders": tables["orders"]})
            if "campaigns" in tables:
                cache.populate({"campaigns": tables["campaigns"]})
            dispatcher.write_all(tables)
            log.info(
                "domain_complete",
                domain=domain_name,
                tables=list(tables.keys()),
                rows={k: len(v) for k, v in tables.items()},
                elapsed=round(time.perf_counter() - t_domain, 2),
            )

    # 6. Summary
    elapsed = round(time.perf_counter() - t_start, 2)
    typer.echo(f"[3/3] Done. Total elapsed: {elapsed}s")
    log.info("generation_complete", profile=profile, elapsed_s=elapsed, crisis_days=len(crisis_days))


@app.command()
def validate(
    output: Annotated[str, typer.Option("--output", "-o", help="Path to generated Parquet/CSV output")] = "./output",
    format: Annotated[str, typer.Option("--format", "-f", help="Output format to validate: parquet | csv")] = "parquet",
) -> None:
    """Run referential integrity checks on generated data."""
    import subprocess

    typer.echo(f"Running referential integrity checks on {output} ({format})...")
    result = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "aurora_data/tests/test_referential_integrity.py",
            "-v",
            "--tb=short",
            f"--output={output}",
            f"--fmt={format}",
        ],
        check=False,
    )
    if result.returncode == 0:
        typer.echo("All checks passed.")
    else:
        typer.echo("Some checks failed. See output above.", err=True)
    raise typer.Exit(result.returncode)


@app.command()
def info(
    config: Annotated[str, typer.Option("--config", "-c")] = DEFAULT_CONFIG,
) -> None:
    """Show profile sizes and output schema summary."""
    from .generators.base import load_config
    cfg = load_config(config)
    typer.echo("Aurora Corp Data Generator\n")
    typer.echo("Profiles:")
    for name, p in cfg.get("profiles", {}).items():
        typer.echo(
            f"  {name:12s}  customers={p['n_customers']:>8,}  orders={p['n_orders']:>12,}  days={p['date_range_days']}"
        )
    typer.echo(f"\nDomains: {', '.join(DOMAIN_MAP.keys())}")
    typer.echo(f"Sinks:   csv | parquet | postgres | all")


if __name__ == "__main__":
    app()

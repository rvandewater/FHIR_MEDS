"""Deprecated `fhir2meds` / `FHIR_MEDS` / `MEDS_ON_FHIR` commands: they run `meds-extract-run spec=FHIR`.

The ETL is the MEDS-Extract spec registered as `FHIR`, run with `meds-extract-run` (see the README). These
commands only translate the arguments of the old converter for one more release, and will be removed:

    fhir2meds raw_input_dir=RAW root_output_dir=ROOT dataset_name=NAME dataset_version=1.0 [fhir_version=R5]

runs

    meds-extract-run spec=FHIR output_dir=ROOT/MEDS_cohort do_download=false input_dir=RAW \
        dataset_version=1.0 "overrides=['etl_metadata.dataset_name=NAME', '++stage_cfg.fhir_version=R5']"

The MEDS cohort stays in `ROOT/MEDS_cohort`; the flattened tables move to `ROOT/MEDS_cohort/fhir_pre_meds`.
"""

import os
import shutil
import sys
import sysconfig
from pathlib import Path

from .stages import PRE_MEDS_DEFAULTS

#: Old converter options that are now options of the `fhir_pre_meds` stage (`max_events` included).
STAGE_OPTIONS = frozenset(PRE_MEDS_DEFAULTS)
#: `meds-extract-run` options, passed on unchanged.
RUN_OPTIONS = frozenset(
    {
        "dataset_version",
        "do_cleanup_intermediates",
        "skip_stages",
        "stage_runner_fp",
        "do_profile",
        "overrides",
    }
)


def _bool(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes"}


def translate(args: list[str], env: dict[str, str] | None = None) -> list[str]:
    """The `meds-extract-run` arguments for the old converter's arguments `args`.

    `DATASET_NAME` and `RAW_DATASET_VERSION` in `env` name the build, as they did for the old converter.

    Examples:
        >>> translate(
        ...     ["raw_input_dir=/raw", "root_output_dir=/root", "dataset_name=X", "fhir_version=R5"], env={}
        ... )
        ['spec=FHIR', 'output_dir=/root/MEDS_cohort', 'do_download=false', 'input_dir=/raw',
         "overrides=['etl_metadata.dataset_name=X','++stage_cfg.fhir_version=R5']"]
        >>> translate(["root_output_dir=/root", "do_download=true", "do_demo=true", "do_enrich=false"],
        ...           env={"RAW_DATASET_VERSION": "2.1.0"})
        ['spec=FHIR', 'output_dir=/root/MEDS_cohort', 'dataset_key=demo', 'download_dest_dir=/root/raw_input',
         'skip_stages=[sumeds_enrich]', 'dataset_version=2.1.0']
        >>> translate(["root_output_dir=/root", "enrich_args=[--strict]"], env={})
        Traceback (most recent call last):
            ...
        ValueError: fhir2meds no longer takes enrich_args=; run meds-extract-run spec=FHIR (see the README).
    """
    env = os.environ if env is None else env
    given = {}
    for arg in args:
        key, sep, value = arg.lstrip("+").partition("=")
        if not sep:
            raise ValueError(f"Expected key=value arguments, got {arg!r}.")
        given[key] = value
    if "root_output_dir" not in given:
        raise ValueError("root_output_dir= is required.")
    root = Path(given.pop("root_output_dir"))
    raw = given.pop("raw_input_dir", str(root / "raw_input"))
    argv = ["spec=FHIR", f"output_dir={root / 'MEDS_cohort'}"]
    do_demo = _bool(given.pop("do_demo", "false"))
    if _bool(given.pop("do_download", "false")):
        argv += [f"dataset_key={'demo' if do_demo else 'mimic'}", f"download_dest_dir={raw}"]
    else:
        argv += ["do_download=false", f"input_dir={raw}"]
    if not _bool(given.pop("do_enrich", "true")):
        argv.append("skip_stages=[sumeds_enrich]")
    given.pop("do_overwrite", None)  # handled by main()
    name = given.pop("dataset_name", env.get("DATASET_NAME"))
    version = given.pop("dataset_version", env.get("RAW_DATASET_VERSION"))
    if version:
        argv.append(f"dataset_version={version}")
    overrides = [f"etl_metadata.dataset_name={name}"] if name else []
    for key, value in given.items():
        if key in STAGE_OPTIONS:
            overrides.append(f"++stage_cfg.{key}={value}")
        elif key in RUN_OPTIONS:
            argv.append(f"{key}={value}")
        else:
            raise ValueError(
                f"fhir2meds no longer takes {key}=; run meds-extract-run spec=FHIR (see the README)."
            )
    if overrides:
        argv.append("overrides=[" + ",".join(f"'{o}'" for o in overrides) + "]")
    return argv


def main() -> None:
    """Run `meds-extract-run spec=FHIR` with the old converter's arguments, after a deprecation warning."""
    from MEDS_extract.run.cli import main as meds_extract_run

    args = sys.argv[1:]
    prog = Path(sys.argv[0]).name
    try:
        argv = translate(args)
    except ValueError as e:
        sys.exit(f"{prog}: {e}")
    print(
        f"{prog} is deprecated and will be removed; it runs:\n  meds-extract-run {' '.join(argv)}",
        file=sys.stderr,
    )
    given = dict(a.lstrip("+").partition("=")[::2] for a in args)
    if _bool(given.get("do_overwrite", "false")):
        # As the old converter did, start from an empty output.
        shutil.rmtree(Path(given["root_output_dir"]) / "MEDS_cohort", ignore_errors=True)
    # MEDS-Transforms starts every stage as `MEDS_transform-stage` from PATH: use this environment's.
    os.environ["PATH"] = os.pathsep.join([sysconfig.get_path("scripts"), os.environ.get("PATH", "")])
    sys.argv = ["meds-extract-run", *argv]
    meds_extract_run()


if __name__ == "__main__":
    main()

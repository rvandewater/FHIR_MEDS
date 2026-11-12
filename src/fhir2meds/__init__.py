from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

__package_name__ = "fhir2meds"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
# The MESSY spec registered as `FHIR` (see pyproject.toml).
MESSY_CFG = files(__package_name__).joinpath("configs/messy.yaml")

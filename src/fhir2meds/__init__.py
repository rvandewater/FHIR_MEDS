from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

from omegaconf import OmegaConf

__package_name__ = "fhir2meds"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
DATASET_CFG = files(__package_name__).joinpath("dataset.yaml")
MAIN_CFG = files(__package_name__).joinpath("configs/main.yaml")
dataset_info = OmegaConf.load(DATASET_CFG)

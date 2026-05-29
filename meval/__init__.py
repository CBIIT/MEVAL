"""MEVAL package compatibility layer."""

from .loader import Loader
from .parser import ModelParser as Parser
from .validator import Validator

__all__ = ["Loader", "Parser", "Validator"]

"""KinopioHub v3: memory-only variables replicated over NATS Core."""

from ._hub import KinopioHub
from ._variables import Scope, Variable
from ._protocol import KinopioError, UNSET

__version__ = "3.0.0"
__all__ = ["LiveChannel", "LiveContext", "KinopioHub", "Scope", "Variable", "KinopioError", "UNSET", "__version__"]

from ._live import LiveChannel, LiveContext

from typing import Optional
from config.settings.base import BackendBaseSettings
from config.settings.environment import Environment


class BackendProdSettings(BackendBaseSettings):
    DESCRIPTION: Optional[str] = "Production Environment."
    ENVIRONMENT: Environment = Environment.PRODUCTION

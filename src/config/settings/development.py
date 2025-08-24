from typing import Optional
from config.settings.base import BackendBaseSettings
from config.settings.environment import Environment


class BackendDevSettings(BackendBaseSettings):
    DESCRIPTION: Optional[str] = "Development Environment."
    DEBUG: bool = True
    ENVIRONMENT: Environment = Environment.DEVELOPMENT

"""Environment-driven configuration with safe local defaults."""
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LEDGERSHIFT_", env_file=".env", extra="ignore")
    env: str = "local"
    data_dir: Path = Path("data")
    seed: int = 42
    customers: int = 100
    orders: int = 500
    fault_rate: float = 0.02
    spark_master: str = "local[2]"

    def validate_ranges(self) -> None:
        if self.customers < 1 or self.orders < 0:
            raise ValueError("customers must be positive and orders cannot be negative")
        if not 0 <= self.fault_rate <= 1:
            raise ValueError("fault_rate must be between 0 and 1")

settings = Settings()

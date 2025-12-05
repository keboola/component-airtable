"""Configuration models for Airtable component."""

from typing import Any

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator


class SyncOptions(BaseModel):
    """Sync options configuration for incremental loading."""

    sync_mode: str = ""
    date_from: str = ""
    date_to: str = ""


class Destination(BaseModel):
    """Destination configuration for output tables."""

    table_name: str = ""
    incremental_loading: bool = True


class Configuration(BaseModel):
    """Main configuration for Airtable component."""

    # Required parameters
    api_key: str = Field(alias="#api_key")

    # Optional parameters - only required for certain actions
    base_id: str = ""
    table_name: str = ""
    view_name: str = ""
    fields: list[str] = Field(default_factory=list)

    # Nested configurations
    sync_options: SyncOptions = Field(default_factory=SyncOptions)
    destination: Destination = Field(default_factory=Destination)

    class Config:
        """Pydantic configuration."""

        populate_by_name = True  # Allow both alias and field name

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        """Validate that API key is provided."""
        if not v or v.strip() == "":
            raise ValueError("API key is required")
        return v

    def __init__(self, **data: Any):
        """Initialize configuration with validation."""
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration validation failed: {', '.join(error_messages)}")

    def is_incremental_sync(self) -> bool:
        """Check if sync mode is incremental."""
        return self.sync_options.sync_mode == "incremental_sync"

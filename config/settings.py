"""Application settings loaded from environment variables."""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    gcp_project_id: str = "ai-agents-go"
    google_application_credentials: str = ""
    spreadsheet_id: str = "18wczPjTPic2GPh0cG_1hwalGXQK3tMzNvU-dmA3aSxk"
    firestore_database: str = "steel-sections"
    port: int = 8080
    service_name: str = "steel-fabrication"
    version: str = "0.1.0"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

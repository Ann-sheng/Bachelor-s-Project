"""
This module's only job is configuration.
     If a setting changes, this is the only file that changes.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ollama_url:            str = "http://localhost:11434/api/generate"
    ollama_model:          str = "sqlcoder"

    db_host:               str = "localhost"
    db_port:               int = 5432
    db_name:               str
    db_user:               str
    db_password:           str
    db_schema:             str = "bl_dm"

    max_question_length:   int = 500
    query_timeout_seconds: int = 300
    max_rows_returned:     int = 500

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def db_config(self) -> dict:
        return {
            "host":            self.db_host,
            "port":            self.db_port,
            "dbname":          self.db_name,
            "user":            self.db_user,
            "password":        self.db_password,
            "options":         f"-c search_path={self.db_schema}",
            "connect_timeout": 10,
        }


settings = Settings()
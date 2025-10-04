from pydantic import BaseModel


class ModelConfig(BaseModel):
    default_model: str = "gpt-5-nano"


config = ModelConfig()

from typing import Optional, List
from pydantic import BaseModel


class InferredItem(BaseModel):
    cleaned_filename: Optional[str] = None
    batch: Optional[str] = None
    entity: Optional[str] = None
    covered_date: Optional[str] = None
    extension: Optional[str] = None


class InferredBatchOutput(BaseModel):
    inferred_batch: List[InferredItem]

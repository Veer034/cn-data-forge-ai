
from pydantic import BaseModel, Field

class DataStorageDto(BaseModel):
    tenantId: str
    documentId: str
    status: str
    
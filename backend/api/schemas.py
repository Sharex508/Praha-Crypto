# # backend/api/schemas.py

# from pydantic import BaseModel
# from typing import Optional
# from datetime import datetime

# class Trading(BaseModel):
#     symbol: str
#     initialPrice: Optional[float] = None
#     highPrice: Optional[float] = None
#     lastPrice: Optional[float] = None
#     margin3: Optional[float] = None
#     margin5: Optional[float] = None
#     margin10: Optional[float] = None
#     margin20: Optional[float] = None
#     purchasePrice: Optional[float] = None
#     stopLossPrice: Optional[float] = None
#     mar3: bool
#     mar5: bool
#     mar10: bool
#     mar20: bool
#     created_at: datetime
#     status: str
#     last_notified_percentage: Optional[float] = None
#     last_notified_decrease_percentage: Optional[float] = None
#     percentage_increase: Optional[float] = None

#     # class Config:
#     #     orm_mode = True

# backend/api/schemas.py

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Trading(BaseModel):
    symbol: str
    initialPrice: Optional[float] = None
    highPrice: Optional[float] = None
    lastPrice: Optional[float] = None
    margin3: Optional[float] = None
    margin5: Optional[float] = None
    margin10: Optional[float] = None
    margin20: Optional[float] = None
    purchasePrice: Optional[float] = None
    stopLossPrice: Optional[float] = None
    mar3: bool
    mar5: bool
    mar10: bool
    mar20: bool
    created_at: datetime
    status: str
    last_notified_percentage: Optional[float] = None
    last_notified_decrease_percentage: Optional[float] = None
    percentage_increase: Optional[float] = None

    class Config:
        allow_population_by_field_name = True  # Allows using field names in addition to aliases


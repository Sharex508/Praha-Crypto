# backend/api/main.py

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from schemas import Trading
from crud import get_trading_data
from typing import List



app = FastAPI(
    title="PRAHA-CRYPTO API",
    description="API for fetching cryptocurrency trading data",
    version="1.0.0"
)

# Configure CORS
origins = [
    "http://localhost:3000",  # React app
    # Add other origins if your frontend is hosted elsewhere
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allow these origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/trading", response_model=List[Trading])
def read_trading_data():
    try:
        data = get_trading_data()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

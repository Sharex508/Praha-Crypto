# backend/api/crud.py

from database import get_db_connection
from schemas import Trading
from typing import List
from psycopg2.extras import RealDictCursor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_trading_data() -> List[Trading]:
    connection = get_db_connection()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    try:
        query = """
            SELECT 
                symbol,
                initialprice AS "initialPrice",
                highprice AS "highPrice",
                lastprice AS "lastPrice",
                margin3,
                margin5,
                margin10,
                margin20,
                purchaseprice AS "purchasePrice",
                stoplossprice AS "stopLossPrice",
                mar3,
                mar5,
                mar10,
                mar20,
                created_at,
                status,
                last_notified_percentage AS "last_notified_percentage",
                last_notified_decrease_percentage AS "last_notified_decrease_percentage",
                CASE 
                    WHEN purchaseprice IS NOT NULL AND purchaseprice != 0 
                    THEN ((lastprice - purchaseprice) / purchaseprice) * 100 
                    ELSE 0 
                END AS percentage_increase
            FROM trading;
        """
        cursor.execute(query)
        records = cursor.fetchall()
        
        if not records:
            logger.info("No records found in the 'trading' table.")
            return []
        
        # Map records to Pydantic models
        res = [Trading(**record) for record in records]
        return res
    except Exception as e:
        logger.error(f"Error fetching trading data: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

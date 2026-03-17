import os
import hvac
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = FastAPI(title="Prescription API", description="Microservice for SCMD Prescription KPIs")

VAULT_ADDR = os.getenv("VAULT_ADDR", "http://vault:8200")
VAULT_TOKEN = os.getenv("VAULT_TOKEN", "root")
VAULT_SECRET_PATH = os.getenv("VAULT_SECRET_PATH", "secret/data/scmd/postgres")

engine = None

def get_db_credentials_from_vault():
    try:
        client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)
        if not client.is_authenticated():
            raise Exception("Vault authentication failed")
        
        read_response = client.read(VAULT_SECRET_PATH)
        if read_response is None:
             raise Exception(f"Secret not found at path: {VAULT_SECRET_PATH}")
             
        data = read_response.get('data', {})
        if 'data' in data:
            data = data['data']
            
        return data
    except Exception as e:
        print(f"Error fetching from vault: {e}")
        return None

def init_db():
    global engine
    creds = get_db_credentials_from_vault()
    if not creds:
        print("Failed to get DB credentials from Vault. DB won't be initialized.")
        return False
        
    try:
        user = creds.get('user', 'postgres')
        password = creds.get('password', 'postgres')
        host = creds.get('host', 'postgres')
        port = creds.get('port', 5432)
        database = creds.get('database', 'scmd_gold')
        
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(db_url)
        print("Database engine initialized successfully.")
        return True
    except Exception as e:
        print(f"Failed to initialize database engine: {e}")
        return False

@app.on_event("startup")
def startup_event():
    init_db()

def get_db():
    if engine is None:
        if not init_db():
            raise HTTPException(status_code=500, detail="Database connection not initialized")
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "prescription_api", "db_initialized": engine is not None}

# --- Prescription KPIs ---

@app.get("/kpis/p1")
def get_total_quantity_per_month():
    """P1: Total quantity dispensed per month (gold_qty_per_month)"""
    if engine is None:
         raise HTTPException(status_code=500, detail="Database not initialized")
         
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT month_display, total_qty FROM gold_qty_per_month ORDER BY month_display;"))
            return [{"month_display": row[0], "total_qty": row[1]} for row in result]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

@app.get("/kpis/p3")
def get_top_drugs_by_quantity():
    """P3: Top 20 drugs by total quantity dispensed (gold_top20_qty)"""
    if engine is None:
         raise HTTPException(status_code=500, detail="Database not initialized")
         
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT vmp_name, total_qty FROM gold_top20_qty ORDER BY total_qty DESC;"))
            return [{"vmp_name": row[0], "total_qty": row[1]} for row in result]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

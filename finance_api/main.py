import os
import hvac
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = FastAPI(title="Finance API", description="Microservice for SCMD Financial KPIs")

VAULT_ADDR = os.getenv("VAULT_ADDR", "http://vault:8200")
VAULT_TOKEN = os.getenv("VAULT_TOKEN", "root")
VAULT_SECRET_PATH = os.getenv("VAULT_SECRET_PATH", "secret/data/scmd/postgres")

engine = None

def get_db_credentials_from_vault():
    try:
        client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)
        if not client.is_authenticated():
            raise Exception("Vault authentication failed")
        
        # In Vault KV v2, the path usually needs to be formatted with 'data'
        # e.g., if path is 'secret/data/scmd/postgres'
        # hvac read method handles it depending on the secret engine version.
        # We assume KV V2 and the token has access.
        read_response = client.read(VAULT_SECRET_PATH)
        if read_response is None:
             raise Exception(f"Secret not found at path: {VAULT_SECRET_PATH}")
             
        # KV v2 responses have a 'data' wrap around the actual secret data
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
    return {"status": "ok", "service": "finance_api", "db_initialized": engine is not None}

# --- Finance KPIs ---

@app.get("/kpis/f1")
def get_total_cost_per_month():
    """F1: Total cost per month (gold_cost_per_month)"""
    if engine is None:
         raise HTTPException(status_code=500, detail="Database not initialized")
         
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT month_display, total_cost FROM gold_cost_per_month ORDER BY month_display;"))
            return [{"month_display": row[0], "total_cost": row[1]} for row in result]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

@app.get("/kpis/f3")
def get_top_drugs_by_cost():
    """F3: Top 20 drugs by total cost (gold_top20_cost)"""
    if engine is None:
         raise HTTPException(status_code=500, detail="Database not initialized")
         
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT vmp_name, total_cost FROM gold_top20_cost ORDER BY total_cost DESC;"))
            return [{"vmp_name": row[0], "total_cost": row[1]} for row in result]
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))

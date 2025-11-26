
import os
import pandas as pd
import gspread
import uuid
from google.oauth2 import service_account
from google.cloud import bigquery

# --- CONFIGURACIÓN ---
# Estas variables se pueden pasar como variables de entorno en Cloud Run
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'testingia-475006')
DATASET_ID = os.environ.get('BQ_DATASET_ID', 'catalogo_datos')
TABLE_ID = os.environ.get('BQ_TABLE_ID', 'descripcion_embeddings')
STAGING_TABLE_ID = 'tds_staging'

SPREADSHEET_ID = os.environ.get('SHEET_ID', '1OOrzGM7oyR9NmervR7xJpW5D7lH-jk1E2wefb7QjO3M')
SHEET_NAME = os.environ.get('SHEET_NAME', 'Hoja1')

BQ_CONNECTION_ID = os.environ.get('BQ_CONNECTION_ID', 'us-central1.vertex-conn')
MODEL_NAME = 'embedding_model'

GENAI_API_KEY = os.environ.get('GENAI_API_KEY', 'AIzaSyAU4lw56M-y2C3C-EkhqhqsJvsqeBWWTxM')

def get_credentials():
    """
    Obtiene credenciales buscando en Secret Manager (Cloud Run) o localmente.
    """
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets', 
        'https://www.googleapis.com/auth/drive', 
        'https://www.googleapis.com/auth/bigquery'
    ]
    
    # 1. Ruta estándar de Secret Manager en Cloud Run
    secret_path = '/secrets/credentials.json'
    if os.path.exists(secret_path): 
        print("🔒 Usando credenciales desde Secret Manager.")
        return service_account.Credentials.from_service_account_file(secret_path, scopes=scopes)
    
    # 2. Ruta para desarrollo local
    local_path = 'credentials.json'
    if os.path.exists(local_path): 
        print("🏠 Usando credenciales locales.")
        return service_account.Credentials.from_service_account_file(local_path, scopes=scopes)
    
    print("⚠️ No se encontraron credenciales explícitas. Intentando usar identidad por defecto...")
    return None

def get_sheet_data():
    print("🔌 Conectando a Google Sheets...")
    creds = get_credentials()
    
    if creds:
        client = gspread.authorize(creds)
    else:
        client = gspread.service_account() # Intento fallback
        
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    
    data = sheet.get_all_records(head=3)
    return pd.DataFrame(data)

def clean_data(df):
    print("🧹 Limpiando y preparando datos...")
    col_desc = 'Descripción de la TDS'
    
    # Verificar columna
    if col_desc not in df.columns:
        raise ValueError(f"No se encontró la columna '{col_desc}'. Columnas disponibles: {df.columns.tolist()}")

    # Eliminar filas sin descripción
    df = df[df[col_desc].fillna('').str.strip() != ''].copy()
    
    # Generar ID único si no existe
    if 'tds_id' not in df.columns:
        df['tds_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
    cols_candidate = ['tds_id', col_desc,'ID', 'Fuente Origen', 'Tipo Origen']
    cols_final = [c for c in cols_candidate if c in df.columns]
    
    return df[cols_final]

def upload_staging_to_bq(df):
    """Sube los datos crudos a una tabla de staging en BigQuery"""
    print(f"🚀 Subiendo {len(df)} filas a Staging ({DATASET_ID}.{STAGING_TABLE_ID})...")
    
    creds = get_credentials()
    client = bigquery.Client(project=PROJECT_ID, credentials=creds) if creds else bigquery.Client(project=PROJECT_ID)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result() # Esperar completado
    print("✅ Staging completado.")
    return client

def execute_bq_ml(client):
    """
    Ejecuta SQL en BigQuery para:
    1. Crear modelo remoto (si no existe).
    2. Generar embeddings masivos usando ML.GENERATE_EMBEDDING.
    3. Indexar la tabla resultante.
    """
    print("🧠 Ejecutando BigQuery ML (Procesamiento en la nube)...")
    
    # 1. Crear el modelo remoto conectado a Vertex AI
    # Usamos 'text-embedding-004' para coincidir con el Frontend (Gemini)
    create_model_sql = f"""
    CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET_ID}.{MODEL_NAME}`
    REMOTE WITH CONNECTION `{PROJECT_ID}.{BQ_CONNECTION_ID}`
    OPTIONS(endpoint = 'text-embedding-004');
    """
    client.query(create_model_sql).result()
    print("   - Modelo remoto configurado/actualizado.")

    # 2. Generar la tabla final con embeddings
    # Toma datos de STAGING -> Pasa por MODELO -> Guarda en TABLE_ID
    print("   - Generando vectores masivos...")
    generate_sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS
    SELECT 
      * EXCEPT(ml_generate_embedding_result, content),
      ml_generate_embedding_result as embedding
    FROM ML.GENERATE_EMBEDDING(
      MODEL `{PROJECT_ID}.{DATASET_ID}.{MODEL_NAME}`,
      (SELECT *, `Descripción de la TDS` as content FROM `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}`),
      STRUCT(TRUE AS flatten_json_output)
    );
    """
    client.query(generate_sql).result()
    print("   - Tabla final de embeddings creada.")

    # 3. Crear Índice Vectorial
    print("   - Creando índice vectorial...")
    index_sql = f"""
    CREATE OR REPLACE VECTOR INDEX `idx_tds_embedding_auto`
    ON `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` (embedding)
    OPTIONS (distance_type = 'COSINE', index_type = 'IVF');
    """
    try:
        client.query(index_sql).result()
        print("✅ Índice recreado con éxito.")
    except Exception as e:
        # Manejo de error para datasets pequeños
        if "min allowed 5000" in str(e): 
            print("⚠️ Dataset pequeño (<5000 filas). No se creó índice (Búsqueda Exacta activada). Esto es correcto.")
        else: 
            print(f"❌ Error al indexar (No crítico para búsqueda): {e}")

if __name__ == "__main__":
    try:
        # 1. Leer y Limpiar
        df = get_sheet_data()
        df_clean = clean_data(df)
        
        # 2. Subir a Staging
        client = upload_staging_to_bq(df_clean)
        
        # 3. Procesar con BQ ML
        execute_bq_ml(client)
        
        print("🎉 PIPELINE FINALIZADO CON ÉXITO")
        
    except Exception as e:
        print(f"❌ ERROR FATAL: {e}")

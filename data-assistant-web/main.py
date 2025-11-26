import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from google.cloud import bigquery
import google.generativeai as genai

# --- CONFIGURACIÓN ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'testingia-475006')
DATASET_ID = os.environ.get('BQ_DATASET_ID', 'catalogo_datos')
TABLE_ID = os.environ.get('BQ_TABLE_ID', 'descripcion_embeddings')
MODEL_ID = os.environ.get("BQ_MODEL_ID", "embedding_model")
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', 'AIzaSyAU4lw56M-y2C3C-EkhqhqsJvsqeBWWTxM')

# Configurar clientes
app = FastAPI(title="Data Assistant API")
templates = Jinja2Templates(directory="templates")
bq_client = bigquery.Client(project=PROJECT_ID)

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# Modelo de datos para la petición
class SearchRequest(BaseModel):
    query: str

@app.get("/")
async def home(request: Request):
    """Sirve la página web (Frontend)"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/api/search")
async def search(request: SearchRequest):
    """Endpoint principal de búsqueda"""
    try:
        user_query = request.query.strip()
        if not user_query:
            raise HTTPException(status_code=400, detail="La consulta no puede estar vacía")

        # 1. Buscar en BigQuery (Vectorización Nativa)
        logging.info(f"Buscando: {user_query}")
        results = search_bigquery(user_query)
        
        # 2. Generar resumen con Gemini (si hay resultados)
        ai_summary = None
        if results and GEMINI_API_KEY:
            ai_summary = generate_gemini_summary(user_query, results[0])

        return {
            "status": "success",
            "results": results,
            "ai_summary": ai_summary
        }

    except Exception as e:
        logging.error(f"Error en búsqueda: {e}")
        return JSONResponse(
            status_code=500, 
            content={"status": "error", "message": str(e)}
        )

def search_bigquery(text):
    """Ejecuta la consulta vectorial nativa en BigQuery"""
    sanitized_text = text.replace("'", "\\'")
    
    sql = f"""
    SELECT 
      base.`ID` as id,
      base.`Descripción de la TDS` as descripcion,
      base.`Fuente Origen` as fuente_origen,
      base.`Tipo Origen` as tipo_origen,
      (1 - distance) as similitud 
    FROM VECTOR_SEARCH(
      TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`,
      'embedding',
      (
        SELECT ml_generate_embedding_result
        FROM ML.GENERATE_EMBEDDING(
          MODEL `{PROJECT_ID}.{DATASET_ID}.{MODEL_ID}`,
          (SELECT '{sanitized_text}' AS content),
          STRUCT(TRUE AS flatten_json_output)
        )
      ),
      top_k => 5,
      distance_type => 'COSINE'
    )
    ORDER BY distance ASC
    """
    
    try:
        query_job = bq_client.query(sql)
        rows = query_job.result()
        
        results = []
        for row in rows:
            results.append({
                "id": row.id,
                "descripcion": row.descripcion,
                "sistema": row.fuente_origen, # Mapeo para el frontend
                "taxonomia": row.tipo_origen, # Mapeo para el frontend
                "similitud": row.similitud
            })
        return results
    except Exception as e:
        logging.error(f"Error SQL BigQuery: {e}")
        raise e

def generate_gemini_summary(query, top_result):
    """Genera el resumen usando la librería oficial de Gemini"""
    try:
        model = genai.GenerativeModel('gemini-2.0-flash')
        prompt = f"""
        Actúa como un asistente de datos experto.
        Contexto: El usuario busca "{query}".
        Mejor coincidencia encontrada en catálogo: "{top_result['descripcion']}" (Sistema: {top_result['sistema']}).
        Tarea: Explica en una sola frase breve y directa por qué esta tabla es útil para lo que busca el usuario.
        """
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        logging.warning(f"Fallo Gemini: {e}")
        return "No se pudo generar el resumen inteligente."

if __name__ == "__main__":
    # Para ejecución local
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
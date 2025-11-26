# Usamos una imagen base ligera de Python
FROM python:3.9-slim

# Instalamos dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiamos los requisitos e instalamos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código del pipeline
COPY pipeline_automation.py .

# Comando para ejecutar el script
CMD ["python", "pipeline_automation.py"]

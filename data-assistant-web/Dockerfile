# Usamos una imagen de Python ligera y moderna
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=True

# Definimos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos las dependencias e instalamos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el código (main.py y la carpeta templates)
COPY . .

# Exponemos el puerto 8080 (Estándar de Cloud Run)
ENV PORT=8080

# Comando de arranque: Inicia el servidor web Uvicorn
# "main:app" significa: busca el archivo main.py y la variable app dentro de él
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
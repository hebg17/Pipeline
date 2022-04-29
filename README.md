# Pipeline
 Manejo de datos abiertos Metrobus CDMX

-- Omitir Ejecución del API: uvicorn main:app --reload
docker-compose up --build

En otra terminal ejecutar el siguiente comando:
docker run -d --name pipeline -p 8000:8000 pipeline_app

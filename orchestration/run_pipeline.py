# orchestration/run_pipeline.py
import sys
import os
from pathlib import Path

# ✅ Asegura acceso a la carpeta src/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from datetime import datetime
import time
import logging
from src import extract, load, transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="pipeline_log.txt",
    filemode="a"
)

def main():
    start_time = datetime.now()
    logging.info("🚀 Inicio del pipeline ELT")

    try:
        logging.info("🔹 Fase 1: Extracción de datos")
        extract.run_all()
        time.sleep(1)

        logging.info("🔹 Fase 2: Carga de datos al Data Warehouse (SQLite)")
        load.run_all()
        time.sleep(1)

        logging.info("🔹 Fase 3: Transformación SQL")
        transform.run_all()
        time.sleep(1)

        end_time = datetime.now()
        logging.info(f"✅ Pipeline completado exitosamente en {end_time - start_time}")

    except Exception as e:
        logging.error(f"❌ Error en el pipeline: {e}")

if __name__ == "__main__":
    main()

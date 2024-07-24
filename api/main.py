# api/main.py
import logging
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import shutil
from tempfile import NamedTemporaryFile
from produccion_ramos import ProduccionRamos

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Set up CORS
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process/")
async def process_file(file: UploadFile = File(...)):
    try:
        tmp_file_path = f"/tmp/{file.filename}"
        with open(tmp_file_path, 'wb') as tmp:
            shutil.copyfileobj(file.file, tmp)

        produccion_ramos = ProduccionRamos(tmp_file_path)
        produccion_ramos.process_data()
        
        output = produccion_ramos.save_to_bytes()
        
        return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={"Content-Disposition": "attachment; filename=processed_data.xlsx"})
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return {"error": str(e)}

# To run the app locally, use the command: uvicorn api.main:app --reload

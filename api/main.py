# main.py
import logging
import shutil
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from tempfile import NamedTemporaryFile
from produccion_ramos import ProduccionRamos

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Set up CORS
origins = [
    "*",  # Allows all origins, change this to restrict access to specific origins
]

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
        with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_file_path = tmp.name

        produccion_ramos = ProduccionRamos(tmp_file_path)
        produccion_ramos.process_data()
        
        output = produccion_ramos.save_to_bytes()
        
        return StreamingResponse(
           output,
           media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
           headers={"Content-Disposition": "attachment; filename=processed_data.xlsx"}
           )
    except Exception as e:
        logging.error(f"Error processing file: {e}")
        return {"error": str(e)}

# To run the app, use the command: uvicorn main:app --reload

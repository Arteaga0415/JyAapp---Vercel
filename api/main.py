# main.py
import shutil
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from tempfile import NamedTemporaryFile
from produccion_ramos import ProduccionRamos

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
    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_file_path = tmp.name

    produccion_ramos = ProduccionRamos(tmp_file_path)
    produccion_ramos.process_data()
    
    output_file_path = 'processed_data.xlsx'
    produccion_ramos.rename_and_save(output_file_path)

    return FileResponse(output_file_path, filename="processed_data.xlsx")

# To run the app, use the command: uvicorn main:app --reload

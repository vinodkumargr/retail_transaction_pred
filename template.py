import os
from pathlib import Path
import logging


logging.basicConfig(
    level = logging.INFO,
    format = "[%(asctime)s : %(levelname)s] : %(message)s"
)


while True:
    project_name = input("Enter the project name : ")
    if project_name != "":
        break
    

logging.info("creating project by name : {project_name}")


files_list = {
    ".github/workflows/.gitkeep",
    ".github/workflows/main.yaml",
    f"{project_name}/__init__.py",
    f"{project_name}/components/__init__.py",
    f"{project_name}/entity/__init__.py",
    f"{project_name}/pipeline/__init__.py",
    f"{project_name}/logger/__init__.py",
    f"{project_name}/config.py",
    f"{project_name}/exception.py",
    f"{project_name}/predictor.py",
    f"{project_name}/utils.py",
    "configs/config.yaml",
    ".gitignore",
    ".env",
    "requirements.txt",
    "setup.py",
    "main.py",
    "data_dump.py"
}


for filepath in files_list:
    filepath = Path(filepath)
    file_dirs , file_names = os.path.split(filepath)
    if file_dirs !="":
        os.makedirs(file_dirs, exist_ok=True)
        logging.info(f"Creating a new directory : {file_dirs} is done......")
    if (not os.path.exists(filepath)):
        with open(filepath, "w") as f:
            pass
            logging.info(f"Creating a file, called: {file_names} in folder: {file_dirs} is done..... ")
    else:
        logging.info(f"Sorry {filepath} already exists.....")
    

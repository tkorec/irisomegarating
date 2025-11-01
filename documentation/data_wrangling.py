from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import os
import json
import re
import pandas as pd


class DataWrangling():

    def __init__(self, client, drive_service):
        self.parent_folder_id = ""
        self.client = client
        self.drive_service = drive_service

    def upload_clients_folders(self):
        # Querying folders within a parent folder
        query = f"'{self.parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = 'false'"  # Placeholder query
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        return folders
    
    def upload_data(self, folder):
        folder_id = folder["id"]
        query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])
        # Recognize the file that is to be uploaded for probability of default modelling
        date_pattern = re.compile(r"\b\d{8}\b")
        file_missing_date_pattern = [file for file in files if not date_pattern.search(file["name"])]
        if len(file_missing_date_pattern) == 0:
            # Report error of no files without date pattern and go to the next folder
            raise ValueError("No files without date pattern found")
        elif len(file_missing_date_pattern) > 1:
            # Report error of multiple files without date pattern and go to the next folder
            raise ValueError("Multiple files without date pattern found")
        else:
            file_missing_date_pattern = file_missing_date_pattern[0]
            file_type_extension = os.path.splitext(file_missing_date_pattern["name"])[1].lower()
        
        if file_type_extension == ".csv":
            data = pd.read_csv(f"https://drive.google.com/uc?export=download&id={file_missing_date_pattern['id']}")
        elif file_type_extension == ".json":
            data = pd.read_json(f"https://drive.google.com/uc?export=download&id={file_missing_date_pattern['id']}")
        elif file_type_extension in [".xls", ".xlsx"]:
            data = pd.read_excel(f"https://drive.google.com/uc?export=download&id={file_missing_date_pattern['id']}")
        else:
            # Report unsupported file type error and go to the next folder
            raise ValueError(f"Unsupported file type: {file_type_extension}")
        
        # Before parsing the data, check if it contains necessary columns - the columns in files might be in
        # a different order or there also might be other extra variables, so we need to check for the presence
        # of columns necessary for modelling, not only the matching structure of expected_columns list
        expected_columns = []
        if not all(col in [c.lower() for c in data.columns] for col in expected_columns):
            missing_columns = [col for col in expected_columns if col not in [c.lower() for c in data.columns]]
            # Report missing columns error and go to the next folder
            raise ValueError(f"Missing columns: {missing_columns}")
        else:
            # Preprocess data
            processed_data = self.preprocess_data(data)

        return processed_data

    def preprocess_data(self, data):
        pass
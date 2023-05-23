import pymongo
import pandas as pd
import numpy as np
import json
import os, sys
from dataclasses import dataclass


mongo_client = pymongo.MongoClient(os.getenv("MONGO_DB_URL"))
TARGET_COLUMN = "Total_price"
UNWANTED_COLUMNS = ['InvoiceNo','StockCode','CustomerID']
#print(f"mongo client: {mongo_client}")
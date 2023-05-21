import pymongo 
import pandas as pd
import json
import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging

client = pymongo.MongoClient("mongodb+srv://vinod:vinod@cluster0.f6mhnlm.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH="/home/vinod/projects/retail_transaction_pred/Online_Retail.csv"
DATABASE_NAME = "online_retail"
COLLECTION_NAME = "online_retail"


if __name__=="__main__":
    try:
        df = pd.read_csv(DATA_FILE_PATH)
        print(f"Rows and columns: {df.shape}")

        df.reset_index(drop = True, inplace = True)

        records = json.loads(df.T.to_json()).values()
        json_record = list(records)
        print(json_record[0])

        client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
    except Exception as e:
        raise RetailException(e,sys)
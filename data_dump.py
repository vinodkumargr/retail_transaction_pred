import pymongo # pip install pymongo
import pandas as pd
import json

client = pymongo.MongoClient("mongodb+srv://vinod:vinod@cluster0.sttg8mr.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH="/home/vinod/projects/retail_transaction_pred/Online_Retail.csv"
DATABASE_NAME = "Retail_transaction"
COLLECTION_NAME = "retail_trans_pred"


if __name__=="__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns: {df.shape}")

    df.reset_index(drop = True, inplace = True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
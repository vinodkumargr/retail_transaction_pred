import pymongo 
import pandas as pd
import json

client = pymongo.MongoClient("mongodb+srv://vinod:vinod@cluster0.f6mhnlm.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH="/home/vinod/projects/retail_transaction_pred/final_data.csv"
DATABASE_NAME = "online_retail"
COLLECTION_NAME = "online_retail"


if __name__=="__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns: {df.shape}")

    df.reset_index(drop = True, inplace = True)
    df.drop(['Unnamed: 0'], axis=1, inplace=True)

    records = json.loads(df.T.to_json()).values()
    json_record = list(records)
    print(json_record[0])

    #client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
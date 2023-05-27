from Retail_transcation.logger import logging
from Retail_transcation.exception import RetailException
from typing import Optional
import os, sys
import pandas as pd
import numpy as np
from Retail_transcation.predictor import ModelResolver
from Retail_transcation import config
from Retail_transcation import utils


PREDICTION_DIR = "Prediction"

def drop_rows_on_condition(df:pd.DataFrame):
    try:
        # drop rows for quantity less than 1 and greater than 40(outliers)
        drop_quantity = df[(df['Quantity'] < 1) | (df['Quantity'] > 40)]
        df = df.drop(drop_quantity.index, axis=0)

        # drop rows for UnitPrice less than 1 and greater than 30(outliers)
        drop_UnitPrice = df[(df['UnitPrice'] < 1) | (df['UnitPrice'] > 30)]
        df = df.drop(drop_UnitPrice.index, axis=0)
        df["UnitPrice"] = df['UnitPrice'].astype(int)

        # drop rows for Description < 300(to avoid creating more features after encoding)
        df['Description']=df['Description'].str.strip()
        drop_Description = df['Description'].value_counts()[df['Description'].value_counts() < 300]
        drop_Description_index=drop_Description.index
        df = df.drop(df[df['Description'].isin(drop_Description_index)].index, axis=0)

        # take top 200 values of description to avoid features problem:
        top_n_values = 200
        top_values = df['Description'].value_counts().nlargest(top_n_values).index.tolist()
        filtered_column = df['Description'].where(df['Description'].isin(top_values), other='Others')
        df['Description'] = filtered_column.values

        #handle Invoice data:
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
      
        df['Day']=df['InvoiceDate'].dt.day
        df['Month']=df['InvoiceDate'].dt.month
        df['Year']=df['InvoiceDate'].dt.year - 2000
        df = df.drop(['InvoiceDate'],axis=1)
        date_col = ['Day', 'Month', 'Year']
        for i in date_col:
            df[i] = df[i].astype(int)

        return df

    except Exception as e:
        raise RetailException(e,sys)


def create_target_column(df:pd.DataFrame):
    try:
        for column in df.columns:
            if column in ['Quantity', 'UnitPrice']:
                df[column] = df[column].astype(int)
        df['Total_price'] = df['Quantity'] * df['UnitPrice']
        return df
    except Exception as e:
        raise RetailException(e, sys)
    



def strat_batch_prediction(input_file_path):
    try:
        
        os.makedirs(PREDICTION_DIR, exist_ok=True)
        model_resolver=ModelResolver(model_registry="saved_models")

        # load data:
        data = pd.read_csv(input_file_path)
        data = data.dropna(axis=0)
        #data = data.drop(columns=[config.UNWANTED_COLUMNS], axis=1)
        data = drop_rows_on_condition(df=data)
        data = create_target_column(df=data)


        transformer = utils.load_object(file_path=model_resolver.get_latest_save_transform_path())
        
        input_feature_names = list(transformer.get_feature_names_out())

        input_arr = transformer.transform(data[input_feature_names])

        model = utils.load_object(file_path=model_resolver.get_latest_save_model_path())
        prediction = model.predict(input_arr)

        data['prediction'] = prediction

        prediction_file_name = os.path.join(PREDICTION_DIR, "prediction_file.csv")
        data.to_csv(path_or_buf=prediction_file_name, index=False, header=True)

        return prediction_file_name

    except Exception as e:
        raise RetailException(e, sys) from e
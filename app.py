import streamlit as st
import pandas as pd
import numpy as np
import pickle as pkl

pickle_data = "/home/vinod/projects/retail_transaction_pred/artifacts/data_transformation/Pre_process_model/single_pred_data.pkl"
pickle_model = "/home/vinod/projects/retail_transaction_pred/artifacts/Model_pusher/saved_models/model.pkl"
transformer = "/home/vinod/projects/retail_transaction_pred/artifacts/Model_pusher/saved_models/transformer.pkl"


model = pkl.load(open(pickle_model, 'rb'))
transformer = pkl.load(open(transformer, "rb"))

st.title("Retail Transaction Total Price Prediction")


# Load the data for the dropdowns
data = pkl.load(open(pickle_data, 'rb'))

# Description
Description = st.selectbox('Description', data['Description'].unique())

# Country
Country = st.selectbox('Country', data['Country'].unique())

unitprice = st.number_input("Expected Unit Price", step=1, value=1)
unitprice = int(unitprice)

quantity = st.number_input("Expected Quantity", step=1, value=1)
quantity = int(quantity)

# Day
Day = st.number_input("Day", step=1, value=1)
Day = int(Day)

# Month
Month = st.number_input("Month", step=1, value=1)
Month = int(Month)

# Year
Year = st.number_input("Year from 2000 (if Year 2012, enter value 12)", step=1, value=1)
Year = int(Year)

if st.button('PREDICT TOTAL PRICE'):
    query_data = {'Description': [Description],
                  'Country': [Country],
                  'Day': [Day],
                  'Month': [Month],
                  'Year': [Year],
                  'UnitPrice':[unitprice],
                  'Quantity':[quantity]}

    df = pd.DataFrame(query_data, index=[0])

    df = transformer.transform(df)

    # Query point
    y_pred = model.predict(df)

    st.header(f"Total Price: {y_pred[0]}")

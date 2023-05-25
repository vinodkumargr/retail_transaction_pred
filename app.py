import streamlit as st
import pandas as pd
import numpy as np
import pickle as pkl

pickle_model = "/home/vinod/projects/retail_transaction_pred/artifacts/model_trainer/model/model.pkl"
pickle_data = "/home/vinod/projects/retail_transaction_pred/artifacts/model_trainer/model/data.pkl"
encoder_file = "/home/vinod/projects/retail_transaction_pred/encoder.pkl"

model = pkl.load(open(pickle_model, 'rb'))
encoder = pkl.load(open(encoder_file, "rb"))

st.title("Retail Transaction Total Price Prediction")

# Load the data for the dropdowns
data = pkl.load(open(pickle_data, 'rb'))

# Description
Description = st.selectbox('Description', data['Description'].unique())

# Country
Country = st.selectbox('Country', data['Country'].unique())

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
                  'Year': [Year]}

    df = pd.DataFrame(query_data)

    # Encode the categorical features
    Description_encoded = encoder.transform(df['Description'].values.reshape(-1, 1))
    Country_encoded = encoder.transform(df['Country'].values.reshape(-1, 1))

    # Create a query array with the encoded features and other numerical features
    query_array = np.concatenate((Description_encoded, Country_encoded,
                                  df[['Day', 'Month', 'Year']].values), axis=1)

    # Ensure query_array has 120 features
    if query_array.shape[1] < 120:
        query_array = np.concatenate((query_array, np.zeros((query_array.shape[0], 120 - query_array.shape[1]))), axis=1)

    # Set the matching description column to 1 and others to 0
    matching_description_col = np.where(encoder.categories_[0] == Description)[0]
    query_array[:, matching_description_col] = 1
    query_array[:, np.arange(query_array.shape[1]) != matching_description_col] = 0

    # Query point
    y_pred = model.predict(query_array)

    st.header(f"Total Price: {y_pred[0]} INR")

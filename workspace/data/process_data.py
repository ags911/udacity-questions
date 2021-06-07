import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    # 1. load datasets
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    
    # 2. merge datasets
    df = pd.merge(messages, categories, how='left', on='id')
    
    return df
    

def clean_data(df):
    # 3. split categories into separate category columns.
    categories = categories['categories'].str.split(';', expand=True)
    row = categories.iloc[:1]
    category_colnames = row.values.tolist()[0] 
    category_colnames = [s.replace('-2', '').replace('-1', '').replace('-0', '') for s in category_colnames]
    categories.columns = category_colnames
    
    # 4. convert category values to just numbers 0 or 1
    for column in categories:
        categories[column] = categories[column].str.strip().str[-1]
        categories[column] = categories[column].astype(int)
        
    categories = categories.replace(to_replace=2, value=1)

    # 5. replace categories column in df with new category columns
    df = df.drop(columns='categories')
    df1 = pd.concat([df, categories], axis=1, join='inner')
    df = df1

    # 5. remove duplicates
    df = df.drop_duplicates()
    
    return df

def save_data(df, database_filename):
    engine = create_engine('sqlite:///'+ database_filename)
    table_name = database_filename.replace(".db","") + "_table"
    df.to_sql(table_name, engine, index=False, if_exists='replace')


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
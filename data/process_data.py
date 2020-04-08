import sys
import pandas as pd
from sqlalchemy.engine import create_engine


def load_data(messages_filepath, categories_filepath):
    df_messages = pd.read_csv(messages_filepath)
    df_categories = pd.read_csv(categories_filepath)
    #merging messages and categories on 'id' column
    df = pd.merge(df_messages, df_categories, on='id')
    return df

def clean_data(df):
    #creating a category for each category value which are found by seperating them
    categories = df['categories'].str.split(';', expand=True)
    #creating category of columns
    category_colnames = categories[:1].squeeze().apply(lambda x: x[:-2])
    #replacing column names in the original dataframe
    categories.columns = category_colnames

    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].apply(lambda x: x[-1])
        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column], downcast="integer")

    df.drop("categories", axis=1, inplace=True)
    #remove duplicates after merging with the categories table
    df = df.join(categories).drop_duplicates()
    return df

def save_data(df, database_filename):
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('DisasterResponse', engine, index=False)  

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
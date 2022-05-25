import pandas as pd
import numpy as np

class Normalize:

    def __init__(self, path):
        """ path to .csv-file, plus empty dataframe where dataframe will be stored
            after the csv-file is read in.
        Args:
            path ([type]): takes in .csv-file
        """
        self.path = path
        self.df = pd.DataFrame
  
    def read_file(self):
        """ method reads .csv-file to a dataframe
        """
        self.df = pd.read_csv(self.path)

    def inspect_data(self):
        """prints first rows, nr of rows and columns and
           how many null-values in each column to get info about what the data looks like.
        """
        print(self.df.head())
        print(self.df.describe())
        print(self.df.info)
        print(self.df.isnull().sum(axis=0))
        
    def split_column(self, column:str):
        """
            Method to split the vaccine column with multiple values into new separate columns with values as zeroes and ones.
            Since there is dublettes of columns, I have decided to add those together.
            It seems to me like SQLite can not handle '&' and '/' when querying so I have decided to delete those characters from the names.
        Args:
            column (str): Name of column to be splitted.    
        """
        # function to split column and fill na-values to zeros instead.
        df = self.df[column].apply(lambda x: pd.value_counts(x.split(",")))
        df.fillna(0, inplace=True)

        ### Jag vet inte hur jag ska lösa nedan på annat sätt än att skriva ut datan inuti metoden och inte i main.py ###
        df['JohnsonJohnson'] = df['Johnson&Johnson'] + df[' Johnson&Johnson']
        df['OxfordAstraZeneca'] = df['Oxford/AstraZeneca'] + df[' Oxford/AstraZeneca']
        df['PfizerBioNTech'] = df['Pfizer/BioNTech'] + df[' Pfizer/BioNTech']
        df['SinopharmBeijing'] = df['Sinopharm/Beijing'] + df[' Sinopharm/Beijing']
        df['SputnikV'] = df['Sputnik V'] + df[' Sputnik V']
        df['Moderna'] = df['Moderna'] + df[' Moderna']
        df['JohnsonJohnson'] = df['Johnson&Johnson'] + df[' Johnson&Johnson']
        # adds new columns to the old dataframe.
        self.df = pd.concat([self.df, df], axis=1)

    def rename_column(self, old_name:str, new_name:str):
        """method to rename a column.

        Args:
            old_name (str): name of column to be renamed
            new_name (str): new name of column
        """
        self.df.rename(columns={old_name: new_name}, inplace=True)
      
    def drop_na(self):
        """method to drop all na-values in dataframe
        """
        self.df.dropna(inplace=True)
     
    def drop_column(self, column:str):
        """method to drop column from dataframe
        Args:
            column (str): name of column to be dropped
        """
        self.df.drop(column, axis=1, inplace=True)
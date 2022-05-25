from numpy import size
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3


class AnalyzeVisualize:
    
    def plot_most_vaccines(self):
        """ Method to answer the question: How many different types of covid-vaccines have been used by each country?
            Includes code to create dataframe from sql-table, manipulating data through pandas and create a plot.
        """
        
        # Database and sql-query defined
        database = 'database.db'
        query = 'SELECT * FROM vaccines'

        # Subset of the vaccines table
        df = self.__sql_table_to_df(database, query)

        # Sums up all the vaccines and sorted by country in descending order
        df['Total_per_country'] = df.sum(axis=1, numeric_only=True)
        df.sort_values('Total_per_country', ascending = False, inplace=True)

        # Using seaborn to create plot
        #sns.set_context('paper')
        f, ax = plt.subplots(figsize=(20, 60))
        sns.set_color_codes('pastel')
        sns.barplot(x ='Total_per_country', y ='country', data = df,
                    label = 'Total number of vaccine brands per country', color = 'b', edgecolor = 'w')
        ax.legend(ncol = 1, loc = 'lower right')
        plt.xlabel('Number of vaccine brands', size=18)
        plt.ylabel('Countries', size=18)
        plt.title('How many different brands of Covid-vaccines each country have used', size=18)
        sns.despine(left = True, bottom = True)
        plt.show()

    
    def __sql_table_to_df(self, database, query):
        """Private method to get subset from sql database to use inside plot_most_vaccines_method
        Args:
            database ([type]): name of database
            query ([type]): query to request data from database.
        Returns:
            [type]: returns a dataframe
        """
        conn = sqlite3.connect(database)
        df = pd.read_sql_query(query, conn)
        return df
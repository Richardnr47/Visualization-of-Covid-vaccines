from pandas import DataFrame
from database_management import DatabaseManagement
from normalization import Normalize
from analyze_visualize import AnalyzeVisualize

def main():
    
    """Main function to read in csv file -> manipulate the data in pandas to get rid of columns not needed, split columns,
       rename columns and delete na-values -> create database and load the cleaned data into seperate tables -> analyze and plotting.
       I have decided to answer the question 'How many different types of covid-vaccines have been used by each country?'
       There were columns for vaccinations of both total and per hundred, it seemed to me that these values were the same, so I decided
       to drop the per hundred columns and just keep the total vaccinations.
       I decided to drop all the remaining na-values after I splitted the data since I thought it would not affect the data that much.
    """
    # Parameters for objects
    path = 'vaccin_covid.csv'
    db = 'database.db'
    table = 'vaccinated'

    # Create object of the class Normalize.
    normalize = Normalize(path)
   
    # Normalization with pandas
    # Read in file
    normalize.read_file()

    # takes a first look at the data to inspect it.
    normalize.inspect_data()

    # Normalize dataframe
    normalize.split_column('vaccines')
    # rename columns with an extra space.
    normalize.rename_column(' Sinovac', 'Sinovac')
    normalize.rename_column(' Soberana02', 'Soberana02')
    normalize.rename_column(' RBD-Dimer', 'RBDDimer')
    normalize.rename_column(' Sinopharm/Wuhan', 'SinopharmWuhan')
    normalize.rename_column(' Sinopharm/HayatVax', 'SinopharmHayatVax')
    # Drop the duplicated columns since these ones have been added together in to new named columns.
    normalize.drop_column(['Johnson&Johnson',
                           ' Oxford/AstraZeneca', ' Pfizer/BioNTech', ' Sinopharm/Beijing',
                           'Oxford/AstraZeneca', ' Sputnik V',
                           'Pfizer/BioNTech', ' Moderna', 'Sinopharm/Beijing',
                           'Sputnik V', ' Johnson&Johnson'])
    # Drop column vaccines in the dataframe since this one is now splitted into multiple columns.
    normalize.drop_column(['vaccines'])
    # Drop columns that have no impact on the data.
    normalize.drop_column(['daily_vaccinations_raw', 'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred', 'daily_vaccinations_per_million'])
    # Drop the rest of the NAs
    normalize.drop_na()


    # Create object of the class DatabaseManagement
    vaccin_db = DatabaseManagement(normalize.df, db, table)
    # Create Database
    vaccin_db.create_database()
    # Create table and load this table with data from file
    vaccin_db.create_table()


    # Data for new table called vaccines.
    new_table_vaccines = "vaccines"
    new_columns_vaccines = """country TEXT PRIMARY KEY,
                              JohnsonJohnson INT, 
                              OxfordAstraZeneca INT,
                              PfizerBioNTech INT, 
                              SinopharmBeijing INT,
                              Sinovac INT,
                              SputnikV INT,
                              Moderna INT, 
                              Covaxin INT,
                              CanSino INT, 
                              SinopharmWuhan INT,
                              Abdala INT, 
                              Soberana02 INT,
                              QazVac INT, 
                              SinopharmHayatVax INT,
                              EpiVacCorona INT, 
                              RBDDimer INT"""

    select_columns_vaccines = """country,
                                 JohnsonJohnson, OxfordAstraZeneca,
                                 PfizerBioNTech, SinopharmBeijing,
                                 Sinovac, SputnikV,
                                 Moderna, Covaxin,
                                 CanSino, SinopharmWuhan,
                                 Abdala, Soberana02,
                                 QazVac, SinopharmHayatVax,
                                 EpiVacCorona, RBDDimer"""

    from_vaccinated = "vaccinated"

    # Data for new table called sources
    new_table_sources = "sources"

    new_columns_sources = """country TEXT PRIMARY KEY,
                             source_name TEXT,
                             source_website TEXT"""

    select_columns_sources = """country,
                                source_name,
                                source_website"""

    # Split the first table in the database into multiple tables and insert the specified data above here.
    vaccin_db.split_tables(new_table_vaccines, new_columns_vaccines, select_columns_vaccines, from_vaccinated)
    vaccin_db.split_tables(new_table_sources, new_columns_sources, select_columns_sources, from_vaccinated)


    # data to be used to query drop columns.
    table = "vaccinated"
    columns = """country TEXT,
                 iso_code TEXT,
                 date TEXT,
                 total_vaccinations REAL,
                 people_vaccinated REAL,
                 people_fully_vaccinated REAL,
                 daily_vaccinations REAL,
                 PRIMARY KEY (country, date)
                 FOREIGN KEY (country)
                    REFERENCES vaccines(country)
                 FOREIGN KEY (country)
                    REFERENCES sources(country)"""
                    
    new_columns = """country, iso_code, date, total_vaccinations,
                     people_vaccinated, people_fully_vaccinated,
                     daily_vaccinations"""

    # drop columns in table where I splitted the tables and using the specified text above.
    vaccin_db.drop_columns(table, columns, new_columns)


    # Create object of the class AnalyzeVisualize
    analyze_visualize = AnalyzeVisualize()
    # Plotting How many different types of covid-vaccines have been used by each country.   
    analyze_visualize.plot_most_vaccines()
    ### Jag lyckades tyvärr inte lösa hur man separerar länderna då det är så många så dom klumpas ihop, men det går att zooma. ###

if __name__ == '__main__':
    main()
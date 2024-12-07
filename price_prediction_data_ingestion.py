import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd
# Using factory design for data ingestion automation
# Abstract class
class DataIngester(ABC):
    @abstractmethod
    def ingest_data(self, extracted_files_source_path: str) -> pd.DataFrame:
        pass
class ZipExtractor:
    # to extract data from compressed zip file
    def zip_extractor(self, zip_source_path : str, extract_destination_path : str) -> list: #list of files in the extracted directory
        if not zip_source_path.endswith(".zip"):
            raise ValueError("The File is not a .zip file")
        with zipfile.ZipFile(zip_source_path, "r") as zipref:
            zipref.extractall(extract_destination_path)
        extracted_files = os.listdir(extract_destination_path)
        if not extracted_files:
            raise FileNotFoundError("No files present in the zip file")
        return extracted_files


# concrete class
class CsvIngester(DataIngester):
    def ingest_data(self,extracted_files_source_path: str) -> pd.DataFrame:
        # reading the csv files
        csv_df = pd.read_csv(extracted_files_source_path)
        return csv_df


# concrete class
class JsonIngester(DataIngester):
    def ingest_data(self, extracted_files_source_path: str) -> pd.DataFrame:
        # reading the json files
        json_df = pd.read_json(extracted_files_source_path)
        return json_df


# Factory class
class DataIngesterFactory:

    def data_ingester(self, file_source_path: str, destination_path: str) -> dict:  # dictionary of various files
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)

        if os.path.isdir(file_source_path):  # checks if the source is already a folder and not zipped
            extracted_files = os.listdir(file_source_path)
        elif file_source_path.endswith(".zip"):
            zip_extractor = ZipExtractor()
            extracted_files = zip_extractor.zip_extractor(file_source_path, destination_path)

            csv_dfs = []
            json_dfs = []

            for extracted_file in extracted_files:
                file_path = os.path.join(destination_path, extracted_file)
                print(file_path)
                if extracted_file.endswith(".csv"):
                    csv_ingester = CsvIngester()
                    csv_df = csv_ingester.ingest_data(file_path)
                    csv_dfs.append(csv_df)

                elif extracted_file.endswith(".json"):
                    json_ingester = JsonIngester()
                    json_df = json_ingester.ingest_data(file_path)
                    json_dfs.append(json_df)


                else:
                    raise ValueError("No ingester found for file type")

            dataframes_dictionary = {
                "csv": csv_dfs if csv_dfs else "No CSV files found",
                "json": json_dfs if json_dfs else "No JSON files found",
            }

            return dataframes_dictionary

        else:
            raise ValueError("Only Zip files are supported")

# usage

if __name__ == '__main__':
    file_source_path = r"C:\Users\Shily\PycharmProjects\pythonProjectinitlal\PDS\price_data.zip"
    destination_path = r"C:\Users\Shily\PycharmProjects\pythonProjectinitlal\PDS\extracted_price_data"
    ingest_data = DataIngesterFactory()
    df_dict = ingest_data.data_ingester(file_source_path, destination_path)
    csv_list = df_dict['csv']
    for csv in csv_list:
        df = csv
        print(df.head())
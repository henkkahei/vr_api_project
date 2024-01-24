import requests
import polars as pl
import duckdb
import json
import matplotlib.pyplot as plt
import seaborn as sns
import sys

from urllib.parse import urljoin
from pathlib import Path
from datetime import date, timedelta

ipynb = False
if sys.argv[0].endswith("ipykernel_launcher.py"):
    ipynb = True
else:
    path = sys.argv[1]

class VRDataIngestion:
    def __init__(self):
        if ipynb:
            self.project_dir = Path().absolute().parent
        else:
            self.project_dir = Path(path).absolute()
        self.duckdb_path = self.project_dir / "data" / "warehouse" / "vr_data.duckdb"
        self.vr_data_path = self.project_dir / "data" / "datalake" / "staging" / "vr_data"
        self.vr_data_path.mkdir(exist_ok=True, parents=True)

        print(self.__str__())
    
    def __str__(self) -> str:
        return f'Project folder: {self.project_dir},\nduckdb_path: {self.duckdb_path},\nvr_data_path: {self.vr_data_path}'

    def giveDates(self, start_date:str, end_date:str):
        """
        Parameters:
        - start_date, end_date: range where the data should be collected
        
        Class variable iso_dates holds this data (end_date inclusive)
        """
        start_date = date.fromisoformat(start_date)
        end_date = date.fromisoformat(end_date)
        daterange = range((end_date - start_date).days + 1)
        self.iso_dates = [(start_date + timedelta(days=x)).isoformat() for x in daterange]
        print(f"\nFetching data between {self.iso_dates[0]} and {self.iso_dates[-1]} ({len(self.iso_dates)} days)")

    def __http(self, method:str, path:str, BASE="https://rata.digitraffic.fi/", **kwargs):
        """
        Get data from VR API as a JSON, internal method
        """
        path = path.lstrip("/")
        url = urljoin(BASE, path)
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def writeJsonToFile(self, path:str="/api/v1/trains/", method:str="GET", encoding:str='utf-8'):
        """
        Get data from VR API between the dates defined in the give_dates method.
        Insert data as JSON files into project's data folder.

        As a default data is fetched from rata.digitraffic.fi/api/v1/trains/{departure_date}.

        Data is divided into yearly and monthly folders.
        """
        year = self.iso_dates[0][:4]
        month = self.iso_dates[0][5:7]
        vr_data_year_month_path = self.project_dir / "data" / "datalake" / "staging" / "vr_data" / year / month
        vr_data_year_month_path.mkdir(exist_ok=True, parents=True)

        for date in self.iso_dates:
            if date[:4] > year or date[5:7] > month:
                year = date[:4]
                month = date[5:7]
                vr_data_year_month_path = self.project_dir / "data" / "datalake" / "staging" / "vr_data" / year / month
                vr_data_year_month_path.mkdir(exist_ok=True, parents=True)

            data = self.__http(method=method, path=f"{path}{date}" if date not in path else path)
            result_json = json.dumps(data)

            json_path = self.vr_data_path / year / month / f"{date}.json"
            json_path.write_text(result_json, encoding=encoding)
    
    def setSchema(self, schema:dict=None):
        """
        Define the schema for Polars DataFrame
        
        The schema is defined by\
        [this](https://rata.digitraffic.fi/swagger/#/trains/getTrainsByDepartureDate)\
        link.
        """
        if schema is None:
            self.schema = {
            "trainNumber": pl.Int64,
            "departureDate": pl.Utf8,
            "operatorUICCode": pl.Int32,
            "operatorShortCode": pl.Utf8,
            "trainType": pl.Utf8,
            "trainCategory": pl.Utf8,
            "commuterLineID": pl.Utf8,
            "runningCurrently": pl.Boolean,
            "cancelled": pl.Boolean,
            "version": pl.Int64,
            "timetableType": pl.Utf8,
            "timetableAcceptanceDate": pl.Utf8,
            "timeTableRows": pl.List(
                pl.Struct({
                    "stationShortCode": pl.Utf8,
                    "stationUICCode": pl.Int32,
                    "countryCode": pl.Utf8,
                    "type": pl.Utf8,
                    "trainStopping": pl.Boolean,
                    "commercialStop": pl.Boolean,
                    "commercialTrack": pl.Utf8,
                    "cancelled": pl.Boolean,
                    "scheduledTime": pl.Utf8,
                    "liveEstimateTime": pl.Utf8,
                    "estimateSource": pl.Utf8,
                    "unknownDelay": pl.Boolean,
                    "actualTime": pl.Utf8,
                    "differenceInMinutes": pl.Int64,
                    "causes": pl.List(
                        pl.Struct(
                            {
                                "passengerTerm": 
                                pl.Struct(
                                    {
                                        "fi": pl.Utf8,
                                        "en": pl.Utf8,
                                        "sv": pl.Utf8
                                    }
                                ),
                                "categoryCode": pl.Utf8,
                                "categoryName": pl.Utf8,
                                "validFrom": pl.Utf8,
                                "validTo": pl.Utf8,
                                "id": pl.Int32,
                                "detailedCategoryCode": pl.Utf8,
                                "detailedCategoryName": pl.Utf8,
                                "thirdCategoryCode": pl.Utf8,
                                "thirdCategoryName": pl.Utf8,
                                "description": pl.Utf8,
                                "categoryCodeId": pl.Int32,
                                "detailedCategoryCodeId": pl.Int32,
                                "thirdCategoryCodeId": pl.Int32
                            }
                        ),
                    ),
                    "trainReady": pl.Struct(
                    {
                        "source": pl.Utf8,
                        "accepted": pl.Boolean,
                        "timestamp": pl.Utf8
                    }
                    ),
                })
            )}
        else:
            self.schema = schema
    
    def jsonFilesToPolars(self) -> pl.DataFrame:
        """
        Read the JSON files and create a Polars DataFrame.

        The DataFrame has to be done with with vstack method (not optimal)
        because of the json file structure (array of jsons).
        Best and fastest way would be to use pl.scan_ndjson to
        load the data as 'lazy'. In order to do this, data should
        be saved in a different way: by train numbers and under each train,
        the data should be saved under respective year and month.

        Faster, lazy method commented in the code.
        """
        self.vr_data_jsons = list(self.vr_data_path.glob("*/*/*.json"))
        self.polars_df = pl.DataFrame()
        # self.polars_df = pl.scan_ndjson(self.vr_data_jsons, schema=self.schema)
        for i in range(0,len(self.vr_data_jsons)):
            row = pl.read_json(self.vr_data_jsons[i], schema=self.schema)
            self.polars_df.vstack(row, in_place=True)
        return self.polars_df

    def sql(self, query:str, read_only:bool=True, returns:bool=False) -> pl.DataFrame:
        """
        Funktio luo tietokantayhteyden kyselyä varten.

        Parametrit:
        - read_only: Jos False, metodia käytetään viemään dataa tietokantaan.\
        Jos True, metodia käytetään vain datan lukemiseen.
        - returns: Jos True, palauttaa DataFramen.
        """
        with duckdb.connect(str(self.duckdb_path), read_only=read_only) as conn:
            df = conn.execute(query).pl()
        if returns:
            return df

    def dataToBronze(self) -> None:
        """
        Method for inserting the data to Bronze medallion
        Incremental data loading has not been implemented for this project
        """
        self.sql("""
            CREATE SCHEMA IF NOT EXISTS medallion_bronze;

            CREATE OR REPLACE TABLE vr_data.medallion_bronze.vr_data_raw AS (
            
                WITH vr_data_ AS (
                        SELECT
                            md5(
                                trainNumber || departureDate || operatorUICCode
                            ) as route_sk, -- Surrogate Key
                            *
                        FROM polars_df
                )

                SELECT * FROM vr_data_
            );
            """, read_only=False)
    
    def sqlShow(self):
        """
        Return database's tables and table information.
        """
        return self.sql("SHOW", returns=True)

class VRDataVisuals:
    def __init__(self) -> None:
        pass
    
    def plotLatenessInfo(self, df):
        trains = [i[0] for i in df[['train_id']].unique().rows()]
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6), layout='tight', sharex=True)

        sns.barplot(data=df[['train_id', 'day', 'KAJ_departure_timeliness', 'HKI_arrival_timeliness']].sort('day'), ax=ax1, x='day', y='KAJ_departure_timeliness', hue='train_id', palette='Set1')
        timeliness_mean = df['KAJ_departure_timeliness'].mean()
        departure_lateness = df['KAJ_departure_timeliness'].sum()
        ax1.axhline(y=timeliness_mean, c='green', label=f'Departure lateness average: {timeliness_mean:.0f} minutes')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=60, ha='right', rotation_mode="anchor")
        ax1.set_title(f'Total departure lateness for trains {trains}\non given time period: {departure_lateness} minutes', fontsize=12)
        ax1.legend()
        ax1.grid(visible=True, axis='y')

        sns.barplot(data=df[['train_id', 'day', 'KAJ_departure_timeliness', 'HKI_arrival_timeliness']].sort('day'), ax=ax2, x='day', y='HKI_arrival_timeliness', hue='train_id', palette='Set1')
        timeliness_mean = df['HKI_arrival_timeliness'].mean()
        arrival_lateness = df['HKI_arrival_timeliness'].sum()
        ax2.axhline(y=timeliness_mean, c='red', label=f'Arrival lateness average: {timeliness_mean:.0f} minutes')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=60, ha='right', rotation_mode="anchor")
        ax2.set_title(f'Total arrival lateness for trains {trains}\non given time period: {arrival_lateness} minutes', fontsize=12)
        ax2.legend()
        ax2.grid(visible=True, axis='y')

        plt.show()

if __name__ == '__main__':
    data = VRDataIngestion()
    
    print("\nFor this project, data will be fetched from a period of week")
    data.giveDates("2023-05-01", "2023-05-07")
    data.writeJsonToFile()
    data.setSchema()
    polars_df = data.jsonFilesToPolars()
    data.dataToBronze()
    print(f"\nView of the database:\n{data.sqlShow()}")
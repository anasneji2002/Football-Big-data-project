from hdfs_src import HdfsClient
import pandas as pd

class HistoricDataService:
    def __init__(self, client):
        self.__hdfsClient = client

    def getleagueData(self, league: str, year: int):
        prev = year -1
        file_name = f"season-{str(prev)[2:]}{str(year)[2:]}.csv" # for example if we want 2000 data the file name should be season-9900.csv
        content = self.__hdfsClient.get_file(f"data/datasets/{league}/{file_name}")
        string_data = content.decode('utf-8')
        df = pd.DataFrame(string_data.split("\n")[:-1]) # remove empty line
        return df
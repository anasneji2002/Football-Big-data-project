import sys
from pathlib import Path
from dotenv import load_dotenv
import os

sys.path.append(str(Path(__file__).resolve().parent.parent))

from hdfs_src.client.HdfsClient import HdfsClient
from hdfs_src.services.HistoricDataService import HistoricDataService

load_dotenv()

ENV_NAME = os.environ["ENVIRONMENT_NAME"]
os.environ["HDFSCLI_CONFIG"] = str(Path(os.getcwd()) / '.hdfscli.cfg')

cl = HdfsClient(ENV_NAME)
service = HistoricDataService(cl)

df = service.getleagueData("bundesliga", 2014)

print(df.head())

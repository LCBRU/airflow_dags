from dotenv import load_dotenv

load_dotenv()

from warehousing.data_download.edge_download import download_edge_studies

download_edge_studies()
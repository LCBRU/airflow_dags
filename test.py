from dotenv import load_dotenv

load_dotenv()

from warehousing.data_download.edge_download import _download_edge_studies

_download_edge_studies()
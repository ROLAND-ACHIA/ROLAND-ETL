import os

BASE_DIR = os.getenv("BASE_DIR")
RAW_DATA_DIR = os.path.join(BASE_DIR, "data/raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data/processed")
AOI_ZIP_PATH = os.path.join(BASE_DIR, "Shape files_AOI/Abong-Mbang_WH.zip")

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

CDSE_USERNAME = os.getenv("CDSE_USERNAME", "rolandachia7@gmail.com")
CDSE_PASSWORD = os.getenv("CDSE_PASSWORD", "AChia672083022@")

WEKEO_USERNAME = os.getenv("WEKEO_USERNAME", "achia10")
WEKEO_PASSWORD = os.getvenv("WEKEO_PASSWORD", "AChia672083022@")

START_DATE = os.getenv("START_DATE", "2024-01-01T00:00:00Z")
END_DATE = os.getenv("END_DATE", "2024-12-31T23:59:59Z")

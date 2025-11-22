from auth import get_token
from extract import Extract
from transform import Transform
from load import Load
from utils.config import AOI_ZIP_PATH

def run():
    print("="*70)
    print("🚀 INTEGRATED ETL PIPELINE")
    print("="*70)

    # Authenticate
    cdse_token = get_token("cdse")
    wekeo_token = get_token("wekeo")

    # Initialize pipeline components
    extractor = Extract(cdse_token, wekeo_token)
    transformer = Transform()
    loader = Load()

    # EXTRACT
    aoi, bbox = extractor.extract_aoi(AOI_ZIP_PATH)
    sentinel_folder = extractor.extract_sentinel2(bbox)
    temp_file = extractor.extract_temperature(bbox)

    # TRANSFORM
    indices = transformer.transform_sentinel2(sentinel_folder, aoi)
    temp_stats = transformer.transform_temperature(temp_file, aoi)

    # LOAD
    loader.load_results(indices, temp_stats)

    print("\n✅ ETL pipeline completed successfully.")

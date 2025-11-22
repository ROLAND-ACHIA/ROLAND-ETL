"""
Unified data extraction class for the ETL package.

Handles:
- AOI shapefile extraction
- Sentinel-2 imagery download (CDSE)
- ERA5 temperature data download (WEkEO)
"""

import os
import zipfile
import geopandas as gpd
from ..utils.config import RAW_DATA_DIR, START_DATE, END_DATE
from ..utils.logging import setup_logger


class Extract:
    """
    Main data extraction handler.

    Attributes
    ----------
    cdse_token : str
        Access token for CDSE platform.
    wekeo_token : str
        Access token for WEkEO platform.
    """

    def __init__(self, cdse_token: str = None, wekeo_token: str = None):
        self.cdse_token = cdse_token
        self.wekeo_token = wekeo_token
        self.logger = setup_logger("extract")

    # ------------------------------------------------------------------
    # AOI EXTRACTION
    # ------------------------------------------------------------------
    def get_aoi(self, zip_path: str):
        """
        Extracts an AOI shapefile from a ZIP archive and loads it as GeoDataFrame.

        Parameters
        ----------
        zip_path : str
            Path to AOI ZIP file.

        Returns
        -------
        tuple
            (aoi_gdf, bbox) where bbox = [minx, miny, maxx, maxy]
        """
        self.logger.info("Extracting AOI shapefile...")

        shp_files = []
        for root, _, files in os.walk(RAW_DATA_DIR):
            shp_files.extend([os.path.join(root, f) for f in files if f.endswith(".shp")])

        if not shp_files:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(RAW_DATA_DIR)
            for root, _, files in os.walk(RAW_DATA_DIR):
                shp_files.extend([os.path.join(root, f) for f in files if f.endswith(".shp")])

        if not shp_files:
            raise FileNotFoundError("No .shp file found in AOI ZIP or raw directory")

        aoi = gpd.read_file(shp_files[0])
        bbox = aoi.to_crs(epsg=4326).total_bounds
        self.logger.info(f"✅ AOI extracted successfully: {os.path.basename(shp_files[0])}")
        return aoi, bbox

    # ------------------------------------------------------------------
    # SENTINEL-2 EXTRACTION
    # ------------------------------------------------------------------
    def get_sentinel2(self, bbox):
        """
        Downloads Sentinel-2 imagery for the given bounding box and time range.

        Parameters
        ----------
        bbox : list or tuple
            [minx, miny, maxx, maxy] of AOI.

        Returns
        -------
        str or None
            Path to extracted Sentinel-2 product folder, or None if download failed.
        """
        if not self.cdse_token:
            self.logger.warning("CDSE token missing. Skipping Sentinel-2 extraction.")
            return None

        self.logger.info("Extracting Sentinel-2 data from CDSE...")
        # TODO: integrate actual API calls
        # Placeholder for real API logic
        product_path = os.path.join(RAW_DATA_DIR, "sentinel2_mock_product")
        os.makedirs(product_path, exist_ok=True)
        self.logger.info(f"✅ Sentinel-2 data extracted to {product_path}")
        return product_path

    # ------------------------------------------------------------------
    # TEMPERATURE EXTRACTION
    # ------------------------------------------------------------------
    def get_temperature(self, bbox):
        """
        Downloads ERA5 temperature data for the given bounding box and date range.

        Parameters
        ----------
        bbox : list or tuple
            [minx, miny, maxx, maxy] of AOI.

        Returns
        -------
        str or None
            Path to temperature data file, or None if download failed.
        """
        if not self.wekeo_token:
            self.logger.warning("WEkEO token missing. Skipping temperature extraction.")
            return None

        self.logger.info("Extracting ERA5 temperature data from WEkEO...")
        # TODO: integrate actual API call for ERA5 data
        # Placeholder for real logic
        temp_file = os.path.join(RAW_DATA_DIR, "era5_mock_temperature.nc")
        with open(temp_file, "w") as f:
            f.write("Mock temperature data for demonstration.")
        self.logger.info(f"✅ ERA5 data extracted to {temp_file}")
        return temp_file

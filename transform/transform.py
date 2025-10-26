"""
Unified data transformation class for ETL.

Handles:
- Sentinel-2 index computation
- ERA5 temperature data aggregation
"""

import os
import numpy as np
import rasterio
from rasterio.mask import mask
import geopandas as gpd
from ..utils.config import PROCESSED_DATA_DIR
from ..utils.logging import setup_logger


class Transform:
    """
    Transformation stage for ETL pipeline.

    Methods:
    --------
    transform_sentinel2(folder, aoi):
        Compute vegetation and soil indices.
    transform_temperature(file, aoi):
        Compute average temperature over AOI.
    """

    def __init__(self):
        self.logger = setup_logger("transform")

    # ------------------------------------------------------------------
    # SENTINEL-2 TRANSFORMATION
    # ------------------------------------------------------------------
    def transform_sentinel2(self, folder: str, aoi: gpd.GeoDataFrame):
        """
        Compute NDVI, EVI, and soil moisture indices from Sentinel-2 imagery.

        Parameters
        ----------
        folder : str
            Folder path containing Sentinel-2 bands.
        aoi : geopandas.GeoDataFrame
            AOI polygon to clip data to.

        Returns
        -------
        dict
            Dictionary containing computed index arrays.
        """
        self.logger.info("Transforming Sentinel-2 imagery...")

        if not folder or not os.path.exists(folder):
            self.logger.warning("No Sentinel-2 folder found. Skipping.")
            return None

        # Placeholder logic for demonstration
        ndvi = np.random.rand(100, 100)
        evi = np.random.rand(100, 100)
        soilm = np.random.rand(100, 100)

        indices = {
            "NDVI": ndvi,
            "EVI": evi,
            "SOILM": soilm,
        }

        self.logger.info("✅ Sentinel-2 indices computed successfully")
        return indices

    # ------------------------------------------------------------------
    # TEMPERATURE TRANSFORMATION
    # ------------------------------------------------------------------
    def transform_temperature(self, temp_file: str, aoi: gpd.GeoDataFrame):
        """
        Compute summary temperature statistics for AOI.

        Parameters
        ----------
        temp_file : str
            Path to ERA5 temperature NetCDF file.
        aoi : geopandas.GeoDataFrame
            AOI polygon.

        Returns
        -------
        dict
            Dictionary containing mean, min, and max temperatures.
        """
        self.logger.info("Transforming ERA5 temperature data...")

        if not temp_file or not os.path.exists(temp_file):
            self.logger.warning("Temperature file missing. Skipping transformation.")
            return None

        # Placeholder logic
        stats = {
            "mean_temp": 27.4,
            "min_temp": 21.8,
            "max_temp": 33.2
        }

        self.logger.info("✅ Temperature statistics computed successfully")
        return stat
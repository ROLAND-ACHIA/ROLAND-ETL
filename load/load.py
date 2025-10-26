"""
Unified data loading class for ETL.

Handles saving transformed products and summaries.
"""

import os
import csv
import numpy as np
from datetime import datetime
from ..utils.config import PROCESSED_DATA_DIR
from ..utils.logging import setup_logger


class Load:
    """
    Loading stage for ETL pipeline.

    Methods
    -------
    save_indices(indices):
        Save NDVI, EVI, SOILM arrays to GeoTIFFs.
    save_temperature(stats):
        Save temperature summary to CSV.
    """

    def __init__(self):
        self.logger = setup_logger("load")
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # SAVE VEGETATION INDICES
    # ------------------------------------------------------------------
    def save_indices(self, indices: dict):
        """
        Save vegetation indices as GeoTIFFs or NPYs.

        Parameters
        ----------
        indices : dict
            Dictionary of numpy arrays for NDVI, EVI, SOILM.
        """
        if not indices:
            self.logger.warning("No indices to save.")
            return

        for name, data in indices.items():
            filename = os.path.join(PROCESSED_DATA_DIR, f"{name.lower()}_{datetime.now():%Y%m%d_%H%M%S}.npy")
            np.save(filename, data)
            self.logger.info(f"✅ Saved {name} -> {filename}")

    # ------------------------------------------------------------------
    # SAVE TEMPERATURE STATS
    # ------------------------------------------------------------------
    def save_temperature(self, stats: dict):
        """
        Save temperature summary to CSV.

        Parameters
        ----------
        stats : dict
            Dictionary containing temperature statistics.
        """
        if not stats:
            self.logger.warning("No temperature stats to save.")
            return

        csv_path = os.path.join(PROCESSED_DATA_DIR, f"temperature_summary_{datetime.now():%Y%m%d_%H%M%S}.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Metric", "Value"])
            for key, val in stats.items():
                writer.writerow([key, val])

        self.logger.info(f"✅ Temperature summary saved to {csv_path}")

    # ------------------------------------------------------------------
    # COMBINED LOADER
    # ------------------------------------------------------------------
    def load_results(self, indices: dict, temp_stats: dict):
        """
        Save both Sentinel-2 and ERA5 results.

        Parameters
        ----------
        indices : dict
            Sentinel-2 computed indices.
        temp_stats : dict
            ERA5 temperature statistics.
        """
        self.logger.info("Saving all ETL results...")
        self.save_indices(indices)
        self.save_temperature(temp_stats)
        self.logger.info("✅ All ETL results saved successfully.")

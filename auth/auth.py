"""
Generic authentication module for ETL.

Supports:
- Copernicus Data Space Ecosystem (CDSE)
- WEkEO platform
"""

import requests
from ..utils.config import (
    CDSE_USERNAME, CDSE_PASSWORD,
    WEKEO_USERNAME, WEKEO_PASSWORD
)


def get_token(provider: str):
    """
    Authenticate with the given provider and return an access token.

    Parameters
    ----------
    provider : str
        One of {"cdse", "wekeo"}.

    Returns
    -------
    str or None
        Access token string if successful, otherwise None.
    """
    provider = provider.lower()

    if provider == "cdse":
        print("🔑 Authenticating with CDSE...")
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        data = {
            "grant_type": "password",
            "username": CDSE_USERNAME,
            "password": CDSE_PASSWORD,
            "client_id": "cdse-public"
        }
    elif provider == "wekeo":
        print("🔑 Authenticating with WEkEO...")
        url = "https://gateway.prod.wekeo2.eu/hda-broker/gettoken"
        data = {"username": WEKEO_USERNAME, "password": WEKEO_PASSWORD}
    else:
        raise ValueError(f"Unsupported provider: {provider}")

    try:
        # CDSE uses form data; WEkEO expects JSON
        response = requests.post(url, data=data if provider == "cdse" else None,
                                 json=data if provider == "wekeo" else None,
                                 timeout=60)
        response.raise_for_status()
        token = response.json().get("access_token")

        if not token:
            print(f"⚠️ Authentication failed: no token returned from {provider}")
            return None

        print(f"✅ {provider.upper()} authentication successful")
        return token

    except Exception as e:
        print(f"⚠️ {provider.upper()} authentication failed: {e}")
        return None

import os
import pandas as pd
import json
from utils.logger import logger

cols = ['symbol', 'expiry', 'ltp', 'atm_iv', 'atm_strike',
       'percent_change', 'type', 'z_score', 'ivp', 'delta', 'iv_stats', 'hv',
       'fair_price', 'fwd_iv', 'pct_change', 'pe_pe_z_score', 'ce_ce_z_score',
       'pe_ce_z_score', 'four_leg_z_score', 'pe_pe_ivp', 'ce_ce_ivp',
       'pe_ce_ivp', 'four_leg_ivp']

cols_2 = ['symbol', 'expiry', 'ltp', 'atm_iv', 'atm_strike',
       'percent_change', 'type', 'z_score', 'ivp', 'delta', 'iv_stats', 'hv',
       'fair_price', 'fwd_iv', 'pct_change', 'pe_pe_z_score', 'ce_ce_z_score',
       'pe_ce_z_score', 'four_leg_z_score', 'pe_pe_ivp', 'ce_ce_ivp',
       'pe_ce_ivp', 'four_leg_ivp']

CWD = os.getcwd()
ASSET_DIR = os.path.join(CWD, "assets")
ASSET_DIR = os.path.abspath(ASSET_DIR)
# print(ASSET_DIR)

# Optional network-based asset directory
NETWORK_ASSET_DIR = os.environ.get("NETWORK_ASSET_DIR")
if NETWORK_ASSET_DIR:
    NETWORK_ASSET_DIR = os.path.abspath(NETWORK_ASSET_DIR)

CONFIG_FILE = open("scan_config.json", "r")
CONFIG = json.load(CONFIG_FILE)
CONFIG_FILE.close()

if os.path.exists(ASSET_DIR):
    logger.info("Found Asset Folder")
else:
    raise Exception("assets folder not found")


def asset2df(filename: str) -> pd.DataFrame:
    """
    Load an asset file into a pandas DataFrame.

    Args:
        filename (str): Name of the file to load.

    Returns:
        pd.DataFrame: Loaded data.
    """
    file_path = os.path.join(ASSET_DIR, filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File '{filename}' not found in assets folder")

    try:
        df = pd.read_csv(file_path)
        return df
    except pd.errors.EmptyDataError:
        logger.warning(f"File '{filename}' is empty")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing file '{filename}': {e}")
        raise FileNotFoundError(f"{filename} not found")


def network_asset2df(filename: str) -> pd.DataFrame:
    """Load a CSV from the NETWORK_ASSET_DIR into a DataFrame.

    Args:
        filename (str): Name of the file to load from the network directory.

    Returns:
        pd.DataFrame: Loaded data.
    """
    if not NETWORK_ASSET_DIR:
        raise EnvironmentError("Environment variable 'NETWORK_ASSET_DIR' is not set")

    file_path = os.path.join(NETWORK_ASSET_DIR, filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File '{filename}' not found in network assets folder"
        )

    try:
        return pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        logger.warning(f"File '{filename}' is empty")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing file '{filename}': {e}")
        raise FileNotFoundError(f"{filename} not found")

def savedf(dirname: str, filename: str, df: pd.DataFrame):
    save_path = os.path.join(ASSET_DIR, dirname, filename)
    df.to_csv(save_path)

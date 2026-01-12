#database/master_contract_db.py

import os
import pandas as pd
import requests
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, Float, Sequence, Index
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from database.auth_db import get_auth_token, Auth
from extensions import socketio
from utils.logging import get_logger

logger = get_logger(__name__)


DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

class SymToken(Base):
    __tablename__ = 'symtoken'
    id = Column(Integer, Sequence('symtoken_id_seq'), primary_key=True)
    symbol = Column(String, nullable=False, index=True)
    brsymbol = Column(String, nullable=False, index=True)
    name = Column(String)
    exchange = Column(String, index=True)
    brexchange = Column(String, index=True)
    token = Column(String, index=True)
    expiry = Column(String)
    strike = Column(Float)
    lotsize = Column(Integer)
    instrumenttype = Column(String)
    tick_size = Column(Float)

    __table_args__ = (Index('idx_symbol_exchange', 'symbol', 'exchange'),)

def init_db():
    logger.info("Initializing Master Contract DB")
    Base.metadata.create_all(bind=engine)

def delete_symtoken_table():
    logger.info("Deleting Symtoken Table")
    SymToken.query.delete()
    db_session.commit()

def copy_from_dataframe(df):
    logger.info("Performing Bulk Insert")
    # Convert DataFrame to a list of dictionaries
    data_dict = df.to_dict(orient='records')

    # Retrieve existing tokens to filter them out from the insert
    existing_tokens = {result.token for result in db_session.query(SymToken.token).all()}

    # Filter out data_dict entries with tokens that already exist
    filtered_data_dict = [row for row in data_dict if row['token'] not in existing_tokens]

    # Insert in bulk the filtered records
    try:
        if filtered_data_dict:
            db_session.bulk_insert_mappings(SymToken, filtered_data_dict)
            db_session.commit()
            logger.info(f"Bulk insert completed successfully with {len(filtered_data_dict)} new records.")
        else:
            logger.info("No new records to insert.")
    except Exception as e:
        logger.error(f"Error during bulk insert: {e}")
        db_session.rollback()


def download_nubra_instruments(output_path):
    """
    Downloads instrument data from Nubra API for NSE and BSE exchanges.
    """
    date = datetime.now().strftime('%Y-%m-%d')

    # Get the logged-in username for nubra broker from database
    auth_obj = Auth.query.filter_by(broker='nubra', is_revoked=False).first()
    if not auth_obj:
        raise Exception("No active Nubra session found. Please login first.")

    login_username = auth_obj.name
    auth_token = get_auth_token(login_username)

    if not auth_token:
        raise Exception(f"No valid auth token found for user '{login_username}'. Please login first.")

    headers = {
        'Authorization': f'Bearer {auth_token}',
        'x-device-id': 'OPENALGO'
    }

    all_data = []

    for exchange in ['NSE', 'BSE']:
        url = f'https://api.nubra.io/refdata/refdata/{date}?exchange={exchange}'
        logger.info(f"Downloading Nubra instruments for {exchange}")

        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code != 200:
            logger.error(f"{exchange} failed: {response.text}")
            continue

        payload = response.json()
        if 'refdata' in payload:
            all_data.extend(payload['refdata'])

    if not all_data:
        raise Exception("No Nubra instruments downloaded")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pd.DataFrame(all_data).to_json(output_path, orient='records')
    logger.info(f"Download complete with {len(all_data)} instruments")


def process_nubra_json(path):
    """
    Processes the Nubra JSON file to fit the existing database schema.
    """
    df = pd.read_json(path)

    # Basic field mappings
    df['token'] = df['token'].astype(str)
    df['lotsize'] = df['lot_size']
    df['tick_size'] = df['tick_size'].astype(float)
    df['name'] = df['asset']
    df['brsymbol'] = df['stock_name']
    df['brexchange'] = df['exchange']

    # Default symbol to brsymbol (for equities and other non-derivatives)
    df['symbol'] = df['brsymbol']

    # Expiry: YYYYMMDD → DD-MMM-YY (uppercase)
    df['expiry'] = pd.to_datetime(
        df['expiry'].astype(str),
        format='%Y%m%d',
        errors='coerce'
    ).dt.strftime('%d-%b-%y').str.upper()

    # Strike price (options) - divide by 100
    df['strike'] = df['strike_price'].fillna(0).astype(float) / 100

    # Instrument type mapping
    df['instrumenttype'] = None
    df.loc[df['derivative_type'] == 'FUT', 'instrumenttype'] = 'FUT'
    df.loc[df['option_type'] == 'CE', 'instrumenttype'] = 'CE'
    df.loc[df['option_type'] == 'PE', 'instrumenttype'] = 'PE'

    # For equities/cash - set instrumenttype to EQ
    df.loc[df['asset_type'] == 'EQUITY', 'instrumenttype'] = 'EQ'

    # Symbol construction (OpenAlgo standard format)
    # Only process records with valid expiry
    valid_expiry = df['expiry'].notna()

    # Futures: SYMBOL[DDMMMYY]FUT
    fut_mask = (df['instrumenttype'] == 'FUT') & valid_expiry
    df.loc[fut_mask, 'symbol'] = (
        df.loc[fut_mask, 'asset'] + df.loc[fut_mask, 'expiry'].str.replace('-', '', regex=False) + 'FUT'
    )

    # Options: SYMBOL[DDMMMYY][Strike][CE/PE]
    opt_mask = df['instrumenttype'].isin(['CE', 'PE']) & valid_expiry
    df.loc[opt_mask, 'symbol'] = (
        df.loc[opt_mask, 'asset']
        + df.loc[opt_mask, 'expiry'].str.replace('-', '', regex=False)
        + df.loc[opt_mask, 'strike'].astype(int).astype(str)
        + df.loc[opt_mask, 'instrumenttype']
    )

    # Return only required columns
    return df[[
        'symbol',
        'brsymbol',
        'name',
        'exchange',
        'brexchange',
        'token',
        'expiry',
        'strike',
        'lotsize',
        'instrumenttype',
        'tick_size',
    ]]


def delete_nubra_temp_data(output_path):
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
            logger.info(f"The temporary file {output_path} has been deleted.")
        else:
            logger.info(f"The temporary file {output_path} does not exist.")
    except Exception as e:
        logger.error(f"An error occurred while deleting the file: {e}")


def master_contract_download():
    logger.info("Downloading Master Contract")
    output_path = 'tmp/nubra.json'
    try:
        download_nubra_instruments(output_path)
        token_df = process_nubra_json(output_path)
        delete_nubra_temp_data(output_path)

        delete_symtoken_table()
        copy_from_dataframe(token_df)

        return socketio.emit('master_contract_download', {'status': 'success', 'message': 'Successfully Downloaded'})

    except Exception as e:
        logger.error(f"{str(e)}")
        return socketio.emit('master_contract_download', {'status': 'error', 'message': str(e)})


def search_symbols(symbol, exchange):
    return SymToken.query.filter(SymToken.symbol.like(f'%{symbol}%'), SymToken.exchange == exchange).all()
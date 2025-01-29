import logging
import json
from typing import List, Optional
from datetime import datetime
from substrateinterface import SubstrateInterface
import psycopg2
from src.core.plugin import Plugin
from src.core.config import ConfigLoader
from src.core.plugin_registry import PluginRegistry
import time

class BlockProcessor:
    def __init__(self, chain_name: str, config_loader: ConfigLoader):
        self.chain_name = chain_name
        self.config = config_loader.get_chain_config(chain_name)
        self.global_config = config_loader.global_config
        self.relay_chain = self.config['chain'].get('relay_chain', 'polkadot')
        
        # Setup database connection
        db_config = config_loader.get_database_config(chain_name)
        self.db_config = {
            'database': 'postgres',
            'database_host': db_config['host'],
            'database_port': db_config['port'],
            'database_name': db_config['name'],
            'database_user': db_config['user'],
            'database_password': db_config['password']
        }
        
        # Import old code functions
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '../../ingest'))
        from database_utils import connect_to_database, create_tables
        from postgres_utils import insert_block_data
        
        self.connect_to_database = connect_to_database
        self.create_tables = create_tables
        self.insert_block_data = insert_block_data
        
        # Setup database connection using old code
        self.db_connection = self.connect_to_database(self.db_config)
        if not self.db_connection:
            raise Exception("Failed to connect to database")
        logging.info("Successfully connected to database")
        
        # Setup substrate connection
        self.substrate = SubstrateInterface(url=self.config['chain']['wss'])
        
        # Create tables using old code
        self._create_tables()
        
        self.current_block = self.config['chain']['start_block']
        self.batch_size = self.global_config['processing']['batch_size']

    def _create_tables(self):
        """Create all necessary database tables."""
        try:
            # Create blocks table using old code
            self.create_tables(self.db_connection, self.db_config, self.chain_name, self.relay_chain)
            
            # Create assets table
            cursor = self.db_connection.cursor()
            create_assets_sql = """
            CREATE TABLE IF NOT EXISTS assets (
                asset_id TEXT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                decimals INTEGER,
                is_native BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_assets_sql)
            
            # Create transfers table
            create_transfers_sql = """
            CREATE TABLE IF NOT EXISTS transfers (
                id SERIAL PRIMARY KEY,
                block_number BIGINT,
                timestamp TIMESTAMP,
                asset_id TEXT REFERENCES assets(asset_id),
                from_address TEXT,
                to_address TEXT,
                amount NUMERIC,
                UNIQUE(block_number, from_address, to_address, asset_id, amount)
            );
            CREATE INDEX IF NOT EXISTS idx_transfers_block_number ON transfers(block_number);
            CREATE INDEX IF NOT EXISTS idx_transfers_from_address ON transfers(from_address);
            CREATE INDEX IF NOT EXISTS idx_transfers_to_address ON transfers(to_address);
            CREATE INDEX IF NOT EXISTS idx_transfers_asset_id ON transfers(asset_id);
            """
            cursor.execute(create_transfers_sql)
            
            # Create DEX operations table
            create_dex_operations_sql = """
            CREATE TABLE IF NOT EXISTS dex_operations (
                id SERIAL PRIMARY KEY,
                block_number BIGINT,
                timestamp TIMESTAMP,
                section TEXT,
                method TEXT,
                phase TEXT,
                -- Common fields for all operations
                trader_address TEXT,
                operation_type TEXT,
                -- Omnipool specific fields
                asset_in TEXT REFERENCES assets(asset_id),
                asset_in_symbol TEXT,
                amount_in NUMERIC,
                asset_out TEXT REFERENCES assets(asset_id),
                asset_out_symbol TEXT,
                amount_out NUMERIC,
                hub_amount_in NUMERIC,
                hub_amount_out NUMERIC,
                asset_fee_amount NUMERIC,
                protocol_fee_amount NUMERIC,
                -- Broadcast specific fields
                filler TEXT,
                filler_type TEXT,
                operation TEXT,
                inputs JSONB,
                outputs JSONB,
                fees JSONB,
                operation_stack JSONB,
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data JSONB,
                UNIQUE(block_number, section, method, trader_address, raw_data)
            );
            CREATE INDEX IF NOT EXISTS idx_dex_operations_block_number ON dex_operations(block_number);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_trader ON dex_operations(trader_address);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_section ON dex_operations(section);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_method ON dex_operations(method);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_timestamp ON dex_operations(timestamp);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_operation_type ON dex_operations(operation_type);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_asset_in_symbol ON dex_operations(asset_in_symbol);
            CREATE INDEX IF NOT EXISTS idx_dex_operations_asset_out_symbol ON dex_operations(asset_out_symbol);
            """
            cursor.execute(create_dex_operations_sql)
            
            # Create broadcast swaps table
            create_broadcast_swaps_sql = """
            CREATE TABLE IF NOT EXISTS broadcast_swaps (
                id SERIAL PRIMARY KEY,
                block_number BIGINT,
                timestamp TIMESTAMP,
                -- Core swap info
                swapper TEXT,
                filler TEXT,
                filler_type TEXT,
                operation TEXT,
                -- Input asset
                input_asset TEXT REFERENCES assets(asset_id),
                input_asset_symbol TEXT,
                input_amount NUMERIC,
                -- Output asset
                output_asset TEXT REFERENCES assets(asset_id),
                output_asset_symbol TEXT,
                output_amount NUMERIC,
                -- Fee information
                total_fee_amount NUMERIC,
                fee_asset TEXT REFERENCES assets(asset_id),
                fee_asset_symbol TEXT,
                fee_destinations JSONB,  -- Array of {destination, amount}
                -- Routing information
                operation_stack JSONB,
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data JSONB,
                UNIQUE(block_number, swapper, input_asset, output_asset, input_amount, output_amount)
            );
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_block_number ON broadcast_swaps(block_number);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_timestamp ON broadcast_swaps(timestamp);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_swapper ON broadcast_swaps(swapper);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_filler ON broadcast_swaps(filler);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_operation ON broadcast_swaps(operation);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_input_asset_symbol ON broadcast_swaps(input_asset_symbol);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_output_asset_symbol ON broadcast_swaps(output_asset_symbol);
            CREATE INDEX IF NOT EXISTS idx_broadcast_swaps_filler_type ON broadcast_swaps(filler_type);
            """
            cursor.execute(create_broadcast_swaps_sql)
            
            # Insert or update known assets
            insert_assets_sql = """
            INSERT INTO assets (asset_id, name, symbol, decimals, is_native)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (asset_id) DO UPDATE SET
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                decimals = EXCLUDED.decimals,
                is_native = EXCLUDED.is_native
            """
            
            # Known assets
            known_assets = [
                ('0', 'HydraDX', 'HDX', 12, True),  # Native token
                ('1', 'Lrna', 'LRNA', 12, False),   # Fee token
                ('5', 'Dai Stablecoin', 'DAI', 18, False),
                ('102', 'USD Coin', 'USDC', 6, False)
            ]
            
            for asset in known_assets:
                cursor.execute(insert_assets_sql, asset)
            
            self.db_connection.commit()
            cursor.close()
            
            logging.info("Successfully created tables and inserted known assets")
        except Exception as e:
            logging.error(f"Error creating tables: {str(e)}", exc_info=True)
            raise

    def _get_asset_info_from_chain(self, asset_id: str) -> Optional[dict]:
        """Query asset information from the chain's asset registry."""
        try:
            # Query the AssetRegistry pallet
            result = self.substrate.query(
                module='AssetRegistry',
                storage_function='Assets',
                params=[int(asset_id)]
            )
            
            if result:
                asset_data = result.value
                logging.info(f"Found asset data from chain: {asset_data}")
                
                # Extract asset information
                name = asset_data.get('name', f"Asset {asset_id}")
                symbol = asset_data.get('symbol', f"ASSET{asset_id}")
                decimals = asset_data.get('decimals', 12)
                
                return {
                    'asset_id': asset_id,
                    'name': name,
                    'symbol': symbol,
                    'decimals': decimals,
                    'is_native': False
                }
            return None
        except Exception as e:
            logging.error(f"Error querying asset info from chain: {str(e)}", exc_info=True)
            return None

    def _save_new_asset(self, asset_info: dict):
        """Save a new asset to the database."""
        try:
            cursor = self.db_connection.cursor()
            
            insert_sql = """
            INSERT INTO assets (asset_id, name, symbol, decimals, is_native)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (asset_id) DO UPDATE SET
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                decimals = EXCLUDED.decimals,
                is_native = EXCLUDED.is_native
            """
            
            cursor.execute(insert_sql, (
                asset_info['asset_id'],
                asset_info['name'],
                asset_info['symbol'],
                asset_info['decimals'],
                asset_info['is_native']
            ))
            
            self.db_connection.commit()
            logging.info(f"Saved new asset: {asset_info}")
            
        except Exception as e:
            self.db_connection.rollback()
            logging.error(f"Error saving new asset: {str(e)}", exc_info=True)
            raise
        finally:
            cursor.close()

    def _get_asset_decimals(self, asset_id: str) -> int:
        """Get decimals for an asset."""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT decimals FROM assets WHERE asset_id = %s", (asset_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return result[0]
            else:
                # Asset not found in database, try to get it from chain
                asset_info = self._get_asset_info_from_chain(asset_id)
                if asset_info:
                    self._save_new_asset(asset_info)
                    return asset_info['decimals']
                else:
                    # Default to 12 decimals if not found
                    logging.warning(f"No decimals found for asset {asset_id}, using default of 12")
                    return 12
        except Exception as e:
            logging.error(f"Error getting asset decimals: {str(e)}", exc_info=True)
            return 12  # Default to 12 decimals on error

    def _get_block_timestamp(self, block_hash) -> int:
        """Get block timestamp from the timestamp extrinsic in the block."""
        try:
            # Get the block
            block = self.substrate.get_block(block_hash=block_hash)
            
            # Find the timestamp extrinsic
            extrinsics = block.get('extrinsics', [])
            
            for extrinsic in extrinsics:
                try:
                    # Access the value property which contains the full extrinsic data
                    extrinsic_data = extrinsic.value
                    
                    # Check if this is a timestamp extrinsic
                    if (extrinsic_data.get('call', {}).get('call_module') == 'Timestamp' and 
                        extrinsic_data.get('call', {}).get('call_function') == 'set'):
                        # Get the timestamp value from call_args
                        call_args = extrinsic_data.get('call', {}).get('call_args', [])
                        if call_args and len(call_args) > 0:
                            return call_args[0].get('value', 0)
                
                except Exception as ex:
                    logging.warning("Error processing extrinsic: %s", str(ex))
                    continue
            
            logging.warning("No timestamp extrinsic found in block %s", block_hash)
            return int(time.time() * 1000)  # Fallback to current time
            
        except Exception as e:
            logging.warning("Failed to get timestamp from chain: %s", str(e), exc_info=True)
            return int(time.time() * 1000)  # Fallback to current time

    def _ensure_asset_exists_and_get_symbol(self, asset_id: str) -> str:
        """Ensure asset exists in database and return its symbol.
        
        Args:
            asset_id: The asset ID to check/create
            
        Returns:
            The asset symbol
        """
        if asset_id is None:
            return None
            
        cursor = self.db_connection.cursor()
        try:
            # Check if asset exists and get symbol
            cursor.execute("SELECT symbol FROM assets WHERE asset_id = %s", (asset_id,))
            result = cursor.fetchone()
            
            if not result:
                # Asset doesn't exist, try to get info from chain
                asset_info = self._get_asset_info_from_chain(asset_id)
                if asset_info:
                    self._save_new_asset(asset_info)
                    return asset_info['symbol']
                else:
                    # If we can't get info, insert a placeholder
                    symbol = f"ASSET{asset_id}"
                    cursor.execute("""
                        INSERT INTO assets (asset_id, name, symbol, decimals, is_native)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (asset_id) DO NOTHING
                    """, (
                        asset_id,
                        f"Asset {asset_id}",
                        symbol,
                        12,  # Default decimals
                        False
                    ))
                    self.db_connection.commit()
                    return symbol
            else:
                return result[0]
        finally:
            cursor.close()

    def _process_dex_events(self, block_number: int, timestamp: int, events: List[dict]) -> None:
        """Process and save DEX-related events from the block."""
        try:
            cursor = self.db_connection.cursor()
            
            insert_sql = """
            INSERT INTO dex_operations (
                block_number,
                timestamp,
                section,
                method,
                phase,
                trader_address,
                operation_type,
                -- Omnipool fields
                asset_in,
                asset_in_symbol,
                amount_in,
                asset_out,
                asset_out_symbol,
                amount_out,
                hub_amount_in,
                hub_amount_out,
                asset_fee_amount,
                protocol_fee_amount,
                -- Broadcast fields
                filler,
                filler_type,
                operation,
                inputs,
                outputs,
                fees,
                operation_stack,
                raw_data
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (block_number, section, method, trader_address, raw_data) DO NOTHING
            """
            
            operations_count = 0
            for event in events:
                section = event.value['module_id']
                method = event.value['event_id']
                
                if section == 'Omnipool' and method in ['BuyExecuted', 'SellExecuted']:
                    try:
                        # Parse event data
                        data = event.value.get('attributes', {})
                        if not isinstance(data, dict):
                            data = json.loads(data)
                            
                        # Get asset IDs and symbols
                        asset_in_id = str(data['asset_in'])
                        asset_out_id = str(data['asset_out'])
                        asset_in_symbol = self._ensure_asset_exists_and_get_symbol(asset_in_id)
                        asset_out_symbol = self._ensure_asset_exists_and_get_symbol(asset_out_id)
                            
                        # Convert timestamp to datetime
                        dt_timestamp = datetime.fromtimestamp(timestamp/1000)
                        
                        # Insert operation
                        cursor.execute(insert_sql, (
                            block_number,
                            dt_timestamp,
                            section,
                            method,
                            event.value.get('phase', ''),
                            data['who'],
                            method,  # operation_type
                            asset_in_id,
                            asset_in_symbol,
                            str(data['amount_in']),
                            asset_out_id,
                            asset_out_symbol,
                            str(data['amount_out']),
                            str(data['hub_amount_in']),
                            str(data['hub_amount_out']),
                            str(data['asset_fee_amount']),
                            str(data['protocol_fee_amount']),
                            None,  # filler
                            None,  # filler_type
                            None,  # operation
                            None,  # inputs
                            None,  # outputs
                            None,  # fees
                            None,  # operation_stack
                            json.dumps(data)  # raw_data
                        ))
                        operations_count += 1
                        
                    except Exception as e:
                        logging.error(f"Error processing Omnipool operation in block {block_number}: {str(e)}")
                        continue
            
            self.db_connection.commit()
            
        except Exception as e:
            self.db_connection.rollback()
            logging.error(f"Error saving DEX operations: {str(e)}", exc_info=True)
            raise
        finally:
            cursor.close()

    def _process_broadcast_swapped_events(self, block_number: int, timestamp: int, events: List[dict]) -> None:
        """Process and save Broadcast.Swapped events from the block."""
        try:
            cursor = self.db_connection.cursor()
            
            insert_sql = """
            INSERT INTO broadcast_swaps (
                block_number,
                timestamp,
                swapper,
                filler,
                filler_type,
                operation,
                input_asset,
                input_asset_symbol,
                input_amount,
                output_asset,
                output_asset_symbol,
                output_amount,
                total_fee_amount,
                fee_asset,
                fee_asset_symbol,
                fee_destinations,
                operation_stack,
                raw_data
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (block_number, swapper, input_asset, output_asset, input_amount, output_amount) DO NOTHING
            """
            
            swaps_count = 0
            for event in events:
                try:
                    if (event.value['module_id'] == 'Broadcast' and 
                        event.value['event_id'] == 'Swapped' and 
                        'event' in event.value):
                        
                        # Get the event data from the correct location
                        data = event.value['event']['attributes']
                        if isinstance(data, str):
                            data = json.loads(data)
                            
                        # Get input/output asset info
                        input_data = data['inputs'][0]  # Assume first input
                        output_data = data['outputs'][0]  # Assume first output
                        fee_data = data['fees'][0]  # Use first fee for asset info
                        
                        input_asset = str(input_data['asset'])
                        output_asset = str(output_data['asset'])
                        fee_asset = str(fee_data['asset'])
                        
                        # Get symbols
                        input_symbol = self._ensure_asset_exists_and_get_symbol(input_asset)
                        output_symbol = self._ensure_asset_exists_and_get_symbol(output_asset)
                        fee_symbol = self._ensure_asset_exists_and_get_symbol(fee_asset)
                        
                        # Calculate total fee amount
                        total_fee_amount = sum(fee['amount'] for fee in data['fees'])
                        
                        # Process fee destinations
                        fee_destinations = []
                        for fee in data['fees']:
                            destination = fee['destination']
                            if isinstance(destination, dict):
                                # Handle Account object
                                destination = destination.get('Account', '')
                            fee_destinations.append({
                                'destination': destination,
                                'amount': str(fee['amount'])
                            })
                        
                        # Get filler type
                        filler_type = data['filler_type']
                        if isinstance(filler_type, dict):
                            # Handle structured filler type like {"Stableswap": 102}
                            filler_type = next(iter(filler_type.keys()))
                            
                        # Convert timestamp to datetime
                        dt_timestamp = datetime.fromtimestamp(timestamp/1000)
                        
                        # Insert swap
                        cursor.execute(insert_sql, (
                            block_number,
                            dt_timestamp,
                            data['swapper'],
                            data['filler'],
                            filler_type,
                            data['operation'],
                            input_asset,
                            input_symbol,
                            str(input_data['amount']),
                            output_asset,
                            output_symbol,
                            str(output_data['amount']),
                            str(total_fee_amount),
                            fee_asset,
                            fee_symbol,
                            json.dumps(fee_destinations),
                            json.dumps(data['operation_stack']),
                            json.dumps(data)
                        ))
                        swaps_count += 1
                        
                except Exception as e:
                    logging.error(f"Error processing event in block {block_number}: {str(e)}")
                    continue
            
            self.db_connection.commit()
            
        except Exception as e:
            self.db_connection.rollback()
            logging.error(f"Error saving Broadcast swaps: {str(e)}", exc_info=True)
            raise
        finally:
            cursor.close()

    def process_block(self, block_number: int) -> bool:
        """Process a single block."""
        try:
            # Fetch block
            block = self.substrate.get_block(block_number=block_number)
            block_hash = block['header']['hash']
            block_timestamp = self._get_block_timestamp(block_hash)
            
            logging.info(f"Processing block {block_number}")
            
            # Get events for this block
            events = self.substrate.get_events(block_hash=block_hash)
            
            # Process DEX operations (Omnipool only)
            self._process_dex_events(block_number, block_timestamp, events)
            
            # Process Broadcast swaps
            self._process_broadcast_swapped_events(block_number, block_timestamp, events)
            
            # Process block data using old code's format
            block_data = {
                'relay_chain': self.relay_chain,
                'chain': self.chain_name,
                'timestamp': block_timestamp,
                'number': str(block_number),
                'hash': block['header']['hash'],
                'parentHash': block['header']['parentHash'],
                'stateRoot': block['header']['stateRoot'],
                'extrinsicsRoot': block['header']['extrinsicsRoot'],
                'authorId': None,
                'finalized': True,
                'onInitialize': {'events': []},
                'onFinalize': {'events': []},
                'logs': block.get('logs', []),
                'extrinsics': []
            }
            
            # Process extrinsics and their events
            processed_event_indices = []  # Keep track of processed event indices
            for idx, extrinsic in enumerate(block.get('extrinsics', [])):
                extrinsic_data = extrinsic.value
                processed_extrinsic = {
                    'method': {
                        'pallet': extrinsic_data.get('call', {}).get('call_module'),
                        'name': extrinsic_data.get('call', {}).get('call_function')
                    },
                    'args': extrinsic_data.get('call', {}).get('call_args', []),
                    'hash': extrinsic_data.get('extrinsic_hash'),
                    'success': True,
                    'paysFee': True,
                    'events': []
                }
                
                # Add events for this extrinsic
                for event_idx, event in enumerate(events):
                    if event.value.get('extrinsic_idx') == idx:
                        event_data = {
                            'method': {
                                'pallet': event.value['module_id'],
                                'name': event.value['event_id']
                            },
                            'data': event.value.get('attributes', [])
                        }
                        processed_extrinsic['events'].append(event_data)
                        processed_event_indices.append(event_idx)
                
                block_data['extrinsics'].append(processed_extrinsic)
            
            # Process remaining events into onInitialize and onFinalize
            for event_idx, event in enumerate(events):
                if event_idx not in processed_event_indices:
                    event_data = {
                        'method': {
                            'pallet': event.value['module_id'],
                            'name': event.value['event_id']
                        },
                        'data': event.value.get('attributes', [])
                    }
                    
                    # Check phase in event.value
                    phase = event.value.get('phase', '')
                    if phase == 'Initialization':
                        block_data['onInitialize']['events'].append(event_data)
                    elif phase == 'ApplyExtrinsic':
                        # Events with ApplyExtrinsic phase but no extrinsic_idx go to onFinalize
                        block_data['onFinalize']['events'].append(event_data)
                    else:
                        # All other events go to onFinalize
                        block_data['onFinalize']['events'].append(event_data)
            
            # Convert nested objects to strings as in old code
            for log in block_data['logs']:
                if 'value' in log:
                    log['value'] = json.dumps(log['value'])

            for event in block_data['onInitialize'].get('events', []):
                event['data'] = json.dumps(event.get('data', {}))

            for event in block_data['onFinalize'].get('events', []):
                event['data'] = json.dumps(event.get('data', {}))

            for extrinsic in block_data['extrinsics']:
                extrinsic['args'] = json.dumps(extrinsic['args'])
                if 'info' in extrinsic:
                    extrinsic['info'] = json.dumps(extrinsic['info'])
                for event in extrinsic.get('events', []):
                    event['data'] = json.dumps(event.get('data', {}))
            
            # Insert block data using old code
            self.insert_block_data(self.db_connection, block_data, self.chain_name, self.relay_chain)
            logging.info(f"Successfully processed block {block_number}")
            return True
            
        except Exception as e:
            logging.error(f"Error processing block {block_number}: {str(e)}", exc_info=True)
            return False

    def process_blocks(self, end_block: Optional[int] = None) -> None:
        """Process blocks from current_block to end_block."""
        while True:
            try:
                # Get current chain head
                chain_head = self.substrate.get_block_number(self.substrate.get_chain_head())
                
                # Determine end block for this batch
                batch_end = min(
                    self.current_block + self.batch_size,
                    end_block if end_block is not None else chain_head
                )
                
                # Process blocks in batch
                for block_num in range(self.current_block, batch_end + 1):
                    success = self.process_block(block_num)
                    if success:
                        self.current_block = block_num + 1
                        logging.info(f"Processed block {block_num}")
                    else:
                        # On failure, retry the same block next time
                        break
                
                # If we've reached the target block, stop
                if end_block and self.current_block > end_block:
                    break
                    
                # If we're in live mode and caught up, wait for new blocks
                if end_block is None and self.current_block > chain_head:
                    time.sleep(self.config['chain']['block_time'])
                    
            except Exception as e:
                logging.error(f"Error in block processing loop: {str(e)}")
                time.sleep(5)  # Wait before retrying 
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
            logging.info("Successfully created tables")
        except Exception as e:
            logging.error(f"Error creating tables: {str(e)}", exc_info=True)
            raise

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

    def process_block(self, block_number: int) -> bool:
        """Process a single block."""
        try:
            # Fetch block
            block = self.substrate.get_block(block_number=block_number)
            block_hash = block['header']['hash']
            block_timestamp = self._get_block_timestamp(block_hash)
            
            # Get events for this block
            events = self.substrate.get_events(block_hash=block_hash)
            
            # Debug: Log unique event phases
            phases = set(event.value.get('phase', '') for event in events)
            logging.info(f"Event phases in block {block_number}: {phases}")
            
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
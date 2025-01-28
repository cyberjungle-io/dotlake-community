from typing import Dict, Any, List, Optional
from src.core.plugin import Plugin
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, BigInteger, Boolean
from sqlalchemy.dialects.postgresql import JSONB
import logging
import json
from datetime import datetime
from sqlalchemy.sql import text

class BlocksPlugin(Plugin):
    def __init__(self, chain_name: str, config: Dict[str, Any]):
        super().__init__(chain_name, config)
        self.chain_name = chain_name
        self.relay_chain = config.get('chain', {}).get('relay_chain', 'polkadot')
        self.blocks_table = self._create_blocks_table()
        logging.info("Initialized BlocksPlugin")

    def _create_blocks_table(self) -> Table:
        return Table(
            'blocks',
            self.metadata,
            Column('block_number', BigInteger, primary_key=True),
            Column('block_hash', String, unique=True),
            Column('parent_hash', String),
            Column('state_root', String),
            Column('extrinsics_root', String),
            Column('extrinsics_count', Integer),
            Column('events_count', Integer),
            Column('timestamp', DateTime, index=True),
            Column('spec_version', Integer),
            Column('validator', String, nullable=True)
        )

    def _register_event_handlers(self):
        # Blocks plugin doesn't handle events
        pass

    def process_event(self, event: Dict[str, Any], block_number: int, timestamp: int):
        # Blocks plugin doesn't process events
        return None

    def create_tables(self, engine):
        """Create the blocks table."""
        metadata = MetaData()
        table_name = f"blocks_{self.relay_chain}_{self.chain_name}"
        
        # Drop existing table if it exists
        with engine.connect() as connection:
            drop_query = text(f"DROP TABLE IF EXISTS {table_name}")
            create_query = text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    relay_chain VARCHAR(255),
                    chain VARCHAR(255),
                    timestamp BIGINT,
                    number VARCHAR(255) PRIMARY KEY,
                    hash VARCHAR(255),
                    parenthash VARCHAR(255),
                    stateroot VARCHAR(255),
                    extrinsicsroot VARCHAR(255),
                    authorid VARCHAR(255),
                    finalized BOOLEAN,
                    oninitialize JSONB,
                    onfinalize JSONB,
                    logs JSONB,
                    extrinsics JSONB
                )
            """)
            
            connection.execute(drop_query)
            connection.execute(create_query)
            connection.commit()
            logging.info(f"Created table {table_name}")

    def process_block(self, block: Dict[str, Any], block_number: int, block_timestamp: int, events_count: int) -> Optional[Dict[str, Any]]:
        """Process a block and prepare it for storage."""
        try:
            # Convert block data to match old schema
            block_data = {
                'relay_chain': self.relay_chain,
                'chain': self.chain_name,
                'timestamp': block_timestamp,
                'number': str(block_number),  # Store as string like old code
                'hash': block['header']['hash'],
                'parentHash': block['header']['parentHash'],
                'stateRoot': block['header']['stateRoot'],
                'extrinsicsRoot': block['header']['extrinsicsRoot'],
                'authorId': None,  # Will be filled if available
                'finalized': True,  # We're processing finalized blocks
                'onInitialize': {'events': []},
                'onFinalize': {'events': []},
                'logs': block.get('logs', []),
                'extrinsics': []
            }

            # Process extrinsics
            for extrinsic in block.get('extrinsics', []):
                extrinsic_data = extrinsic.value
                processed_extrinsic = {
                    'method': {
                        'pallet': extrinsic_data.get('call', {}).get('call_module'),
                        'name': extrinsic_data.get('call', {}).get('call_function')
                    },
                    'args': extrinsic_data.get('call', {}).get('call_args', []),
                    'hash': extrinsic_data.get('extrinsic_hash'),
                    'success': True,  # Default to true as in old code
                    'paysFee': True,  # Default to true as in old code
                    'events': []
                }
                block_data['extrinsics'].append(processed_extrinsic)

            # Convert all nested objects to strings as in old code
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

            return {
                'table': f"blocks_{self.relay_chain}_{self.chain_name}",
                'data': block_data,
                'update': False
            }

        except Exception as e:
            logging.error(f"Error processing block {block_number}: {str(e)}", exc_info=True)
            return None

    def get_table_names(self) -> List[str]:
        return ['blocks'] 
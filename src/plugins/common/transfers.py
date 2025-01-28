from typing import Dict, Any, List
from src.core.plugin import Plugin
import logging
from datetime import datetime

class TransferPlugin(Plugin):
    def __init__(self, chain_name: str, config: Dict[str, Any]):
        super().__init__(chain_name, config)
        self.table_name = 'transfers'
        logging.info(f"Created transfers table definition: {self.table_name}")

    def process_event(self, event: Dict[str, Any], block_number: int, timestamp: int):
        event_type = f"{event['module']}.{event['event']}"
        if event_type == 'balances.Transfer':
            logging.info(f"Processing transfer event: {event}")
            try:
                return self._handle_transfer(event, block_number, datetime.fromtimestamp(timestamp/1000))
            except Exception as e:
                logging.error(f"Error processing transfer event: {str(e)}", exc_info=True)
        return None

    def _handle_transfer(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        try:
            data = event['data']
            logging.info(f"Processing transfer data: {data}")
            
            # Extract transfer details
            from_address = str(data[0])  # Convert to string in case it's a special type
            to_address = str(data[1])
            amount = int(data[2])  # Amount is the third parameter
            
            return {
                'table': self.table_name,
                'data': {
                    'block_number': block_number,
                    'extrinsic_index': event.get('extrinsic_index'),
                    'event_index': event.get('event_index'),
                    'from_address': from_address,
                    'to_address': to_address,
                    'amount': self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                    'asset_id': None,  # Native token transfer
                    'timestamp': timestamp
                },
                'update': False
            }
        except Exception as e:
            logging.error(f"Error handling transfer: {str(e)}", exc_info=True)
            return None

    def get_table_names(self) -> List[str]:
        """Get list of table names."""
        return [self.table_name]

    def create_tables(self, connection):
        """Create the transfers table using raw SQL."""
        cursor = connection.cursor()
        try:
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            
            # Create table
            create_table_sql = f"""
            CREATE TABLE {self.table_name} (
                id SERIAL PRIMARY KEY,
                block_number BIGINT,
                extrinsic_index INTEGER,
                event_index INTEGER,
                from_address TEXT,
                to_address TEXT,
                amount NUMERIC,
                asset_id TEXT,
                timestamp TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            
            # Create indexes
            cursor.execute(f"CREATE INDEX idx_{self.table_name}_block_number ON {self.table_name}(block_number)")
            cursor.execute(f"CREATE INDEX idx_{self.table_name}_from_address ON {self.table_name}(from_address)")
            cursor.execute(f"CREATE INDEX idx_{self.table_name}_to_address ON {self.table_name}(to_address)")
            cursor.execute(f"CREATE INDEX idx_{self.table_name}_timestamp ON {self.table_name}(timestamp)")
            
            connection.commit()
            logging.info(f"Successfully created {self.table_name} table and indexes")
        except Exception as e:
            connection.rollback()
            logging.error(f"Error creating {self.table_name} table: {str(e)}", exc_info=True)
            raise
        finally:
            cursor.close() 
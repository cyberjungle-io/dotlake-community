from typing import Dict, Any, List
from src.core.plugin import Plugin
from sqlalchemy import Table, Column, Integer, String, Numeric, DateTime, MetaData, BigInteger

class OmnipoolPlugin(Plugin):
    def __init__(self, chain_name: str, config: Dict[str, Any]):
        super().__init__(chain_name, config)
        self.metadata = MetaData()
        self.trades_table = self._create_trades_table()
        self.pools_table = self._create_pools_table()

    def _create_trades_table(self) -> Table:
        return Table(
            'omnipool_trades',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('block_number', BigInteger, index=True),
            Column('extrinsic_index', Integer),
            Column('event_index', Integer),
            Column('asset_in', String, index=True),
            Column('asset_out', String, index=True),
            Column('amount_in', Numeric),
            Column('amount_out', Numeric),
            Column('fee', Numeric),
            Column('timestamp', DateTime, index=True)
        )

    def _create_pools_table(self) -> Table:
        return Table(
            'omnipool_pools',
            self.metadata,
            Column('asset_id', String, primary_key=True),
            Column('creation_block', BigInteger),
            Column('initial_price', Numeric),
            Column('initial_liquidity', Numeric),
            Column('current_liquidity', Numeric),
            Column('last_updated', DateTime),
            Column('created_at', DateTime)
        )

    def _register_event_handlers(self):
        self.event_handlers = {
            'omnipool.PoolCreated': self._handle_pool_created,
            'omnipool.TradeExecuted': self._handle_trade_executed,
            'omnipool.LiquidityAdded': self._handle_liquidity_added
        }

    def process_event(self, event: Dict[str, Any], block_number: int, timestamp: int):
        event_type = f"{event['section']}.{event['method']}"
        if event_type in self.event_handlers:
            return self.event_handlers[event_type](event, block_number, timestamp)
        return None

    def _handle_pool_created(self, event: Dict[str, Any], block_number: int, timestamp: int):
        data = event['data']
        return {
            'table': 'omnipool_pools',
            'data': {
                'asset_id': data['asset_id'],
                'creation_block': block_number,
                'initial_price': self.transform_value(data['initial_price'], "value / (10 ^ 12)"),
                'initial_liquidity': self.transform_value(data['initial_liquidity'], "value / (10 ^ chain.asset_decimals)"),
                'current_liquidity': self.transform_value(data['initial_liquidity'], "value / (10 ^ chain.asset_decimals)"),
                'last_updated': timestamp,
                'created_at': timestamp
            }
        }

    def _handle_trade_executed(self, event: Dict[str, Any], block_number: int, timestamp: int):
        data = event['data']
        return {
            'table': 'omnipool_trades',
            'data': {
                'block_number': block_number,
                'extrinsic_index': event.get('extrinsic_index'),
                'event_index': event.get('event_index'),
                'asset_in': data['asset_in'],
                'asset_out': data['asset_out'],
                'amount_in': self.transform_value(data['amount_in'], "value / (10 ^ chain.asset_decimals)"),
                'amount_out': self.transform_value(data['amount_out'], "value / (10 ^ chain.asset_decimals)"),
                'fee': self.transform_value(data['fee'], "value / (10 ^ chain.asset_decimals)"),
                'timestamp': timestamp
            }
        }

    def _handle_liquidity_added(self, event: Dict[str, Any], block_number: int, timestamp: int):
        # Update the current liquidity in the pools table
        data = event['data']
        return {
            'table': 'omnipool_pools',
            'data': {
                'asset_id': data['asset_id'],
                'current_liquidity': self.transform_value(data['amount'], "value / (10 ^ chain.asset_decimals)"),
                'last_updated': timestamp
            },
            'update': True
        }

    def get_table_names(self) -> List[str]:
        return ['omnipool_trades', 'omnipool_pools']

    def create_tables(self, connection):
        self.metadata.create_all(connection) 
from typing import Dict, Any, List
from src.core.plugin import Plugin
from sqlalchemy import Table, Column, Integer, String, Numeric, DateTime, MetaData, BigInteger
import logging
from datetime import datetime

class BalancesPlugin(Plugin):
    def __init__(self, chain_name: str, config: Dict[str, Any]):
        super().__init__(chain_name, config)
        self.metadata = MetaData()
        self.account_balances_table = self._create_account_balances_table()
        self.balance_history_table = self._create_balance_history_table()

    def _create_account_balances_table(self) -> Table:
        return Table(
            'account_balances',
            self.metadata,
            Column('account', String, primary_key=True),
            Column('free_balance', Numeric),
            Column('reserved_balance', Numeric),
            Column('total_balance', Numeric),
            Column('nonce', BigInteger),
            Column('last_updated_block', BigInteger),
            Column('last_updated_at', DateTime),
            Column('created_at', DateTime)
        )

    def _create_balance_history_table(self) -> Table:
        return Table(
            'balance_history',
            self.metadata,
            Column('id', Integer, primary_key=True),
            Column('account', String, index=True),
            Column('block_number', BigInteger, index=True),
            Column('extrinsic_index', Integer),
            Column('event_index', Integer),
            Column('free_balance', Numeric),
            Column('reserved_balance', Numeric),
            Column('total_balance', Numeric),
            Column('timestamp', DateTime, index=True)
        )

    def _register_event_handlers(self):
        self.event_handlers = {
            'balances.BalanceSet': self._handle_balance_set,
            'balances.Deposit': self._handle_deposit,
            'balances.Withdraw': self._handle_withdraw,
            'balances.Reserved': self._handle_reserved,
            'balances.Unreserved': self._handle_unreserved,
            'balances.Transfer': self._handle_transfer,
            'system.NewAccount': self._handle_new_account
        }

    def process_event(self, event: Dict[str, Any], block_number: int, timestamp: int):
        event_type = f"{event['module']}.{event['event']}"
        handler = self.event_handlers.get(event_type)
        
        if handler:
            logging.info(f"Processing balance event: {event_type}")
            try:
                return handler(event, block_number, datetime.fromtimestamp(timestamp/1000))
            except Exception as e:
                logging.error(f"Error processing balance event: {str(e)}", exc_info=True)
        return None

    def _handle_balance_set(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        account = data[0]
        free_balance = int(data[1])
        
        return [
            {
                'table': 'account_balances',
                'data': {
                    'account': account,
                    'free_balance': self.transform_value(free_balance, "value / (10 ^ chain.asset_decimals)"),
                    'reserved_balance': 0,
                    'total_balance': self.transform_value(free_balance, "value / (10 ^ chain.asset_decimals)"),
                    'last_updated_block': block_number,
                    'last_updated_at': timestamp
                },
                'update': True
            },
            {
                'table': 'balance_history',
                'data': {
                    'account': account,
                    'block_number': block_number,
                    'extrinsic_index': event.get('extrinsic_index'),
                    'event_index': event.get('event_index'),
                    'free_balance': self.transform_value(free_balance, "value / (10 ^ chain.asset_decimals)"),
                    'reserved_balance': 0,
                    'total_balance': self.transform_value(free_balance, "value / (10 ^ chain.asset_decimals)"),
                    'timestamp': timestamp
                },
                'update': False
            }
        ]

    def _handle_deposit(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        account = data[0]
        amount = int(data[1])
        
        return {
            'table': 'balance_history',
            'data': {
                'account': account,
                'block_number': block_number,
                'extrinsic_index': event.get('extrinsic_index'),
                'event_index': event.get('event_index'),
                'amount': self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                'timestamp': timestamp
            },
            'update': False
        }

    def _handle_withdraw(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        account = data[0]
        amount = int(data[1])
        
        return {
            'table': 'balance_history',
            'data': {
                'account': account,
                'block_number': block_number,
                'extrinsic_index': event.get('extrinsic_index'),
                'event_index': event.get('event_index'),
                'amount': -self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                'timestamp': timestamp
            },
            'update': False
        }

    def _handle_reserved(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        account = data[0]
        amount = int(data[1])
        
        return {
            'table': 'balance_history',
            'data': {
                'account': account,
                'block_number': block_number,
                'extrinsic_index': event.get('extrinsic_index'),
                'event_index': event.get('event_index'),
                'reserved_amount': self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                'timestamp': timestamp
            },
            'update': False
        }

    def _handle_unreserved(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        account = data[0]
        amount = int(data[1])
        
        return {
            'table': 'balance_history',
            'data': {
                'account': account,
                'block_number': block_number,
                'extrinsic_index': event.get('extrinsic_index'),
                'event_index': event.get('event_index'),
                'reserved_amount': -self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                'timestamp': timestamp
            },
            'update': False
        }

    def _handle_transfer(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        from_account = data[0]
        to_account = data[1]
        amount = int(data[2])
        
        return [
            {
                'table': 'balance_history',
                'data': {
                    'account': from_account,
                    'block_number': block_number,
                    'extrinsic_index': event.get('extrinsic_index'),
                    'event_index': event.get('event_index'),
                    'amount': -self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                    'timestamp': timestamp
                },
                'update': False
            },
            {
                'table': 'balance_history',
                'data': {
                    'account': to_account,
                    'block_number': block_number,
                    'extrinsic_index': event.get('extrinsic_index'),
                    'event_index': event.get('event_index'),
                    'amount': self.transform_value(amount, "value / (10 ^ chain.asset_decimals)"),
                    'timestamp': timestamp
                },
                'update': False
            }
        ]

    def _handle_new_account(self, event: Dict[str, Any], block_number: int, timestamp: datetime):
        data = event['data']
        account = data[0]
        
        return {
            'table': 'account_balances',
            'data': {
                'account': account,
                'free_balance': 0,
                'reserved_balance': 0,
                'total_balance': 0,
                'nonce': 0,
                'last_updated_block': block_number,
                'last_updated_at': timestamp,
                'created_at': timestamp
            },
            'update': False
        }

    def get_table_names(self) -> List[str]:
        return ['account_balances', 'balance_history']

    def create_tables(self, connection):
        self.metadata.create_all(connection) 
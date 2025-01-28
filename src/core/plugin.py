from abc import ABC, abstractmethod
from typing import Dict, Any, List
from sqlalchemy import MetaData

class Plugin(ABC):
    def __init__(self, chain_name: str, config: Dict[str, Any]):
        self.chain_name = chain_name
        self.config = config
        self.metadata = MetaData()
        self.event_handlers = {}
        self._register_event_handlers()

    @abstractmethod
    def _register_event_handlers(self):
        """Register event handlers for this plugin."""
        pass

    @abstractmethod
    def process_event(self, event: Dict[str, Any], block_number: int, timestamp: int):
        """Process a single event."""
        pass

    @abstractmethod
    def get_table_names(self) -> List[str]:
        """Get list of table names this plugin manages."""
        return []

    def create_tables(self, engine):
        """Create all tables for this plugin."""
        try:
            self.metadata.create_all(engine, checkfirst=True)
        except Exception as e:
            raise Exception(f"Failed to create tables for {self.__class__.__name__}: {str(e)}")

    def transform_value(self, value: Any, transform_expr: str) -> Any:
        """Transform a value using the provided expression."""
        # This is a placeholder. In a real implementation, you would evaluate the expression
        return value

    def transform_value(self, value: Any, transform: str) -> Any:
        """Apply transformation to a value based on configuration."""
        if not transform:
            return value
        
        # Get chain-specific configuration
        chain_config = self.config.get('chain_specific', {})
        
        # Create a context with chain configuration
        context = {
            'chain': chain_config,
            'value': value
        }
        
        # Evaluate the transform expression
        return eval(transform, {"__builtins__": {}}, context) 
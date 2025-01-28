from typing import Dict, List, Type
from src.core.plugin import Plugin
from src.plugins.common.transfers import TransferPlugin
from src.plugins.common.blocks import BlocksPlugin
from src.plugins.common.balances import BalancesPlugin
from src.plugins.hydration.omnipool import OmnipoolPlugin

class PluginRegistry:
    def __init__(self):
        self.available_plugins: Dict[str, Type[Plugin]] = {
            'common.transfers': TransferPlugin,
            'common.blocks': BlocksPlugin,
            'common.balances': BalancesPlugin,
            'hydration.omnipool': OmnipoolPlugin,
        }
        self.active_plugins: Dict[str, Plugin] = {}

    def load_plugins(self, chain_name: str, enabled_plugins: List[str], chain_config: dict) -> List[Plugin]:
        """Load and initialize enabled plugins for a chain."""
        loaded_plugins = []
        for plugin_name in enabled_plugins:
            if plugin_name in self.available_plugins:
                plugin_class = self.available_plugins[plugin_name]
                plugin_instance = plugin_class(chain_name, chain_config)
                self.active_plugins[plugin_name] = plugin_instance
                loaded_plugins.append(plugin_instance)
            else:
                raise ValueError(f"Plugin {plugin_name} not found in registry")
        return loaded_plugins

    def get_plugin(self, plugin_name: str) -> Plugin:
        """Get an active plugin instance by name."""
        return self.active_plugins.get(plugin_name)

    def get_active_plugins(self) -> List[Plugin]:
        """Get all active plugin instances."""
        return list(self.active_plugins.values()) 

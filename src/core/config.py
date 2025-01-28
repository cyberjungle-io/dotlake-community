import os
import yaml
from typing import Dict, Any
from dotenv import load_dotenv

class ConfigLoader:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Load global config
        with open('config/global.yaml', 'r') as f:
            self.global_config = yaml.safe_load(f)
            
        # Load chain configs
        self.chain_configs = {}
        self._load_chain_configs()
        
    def _load_chain_configs(self):
        """Load all chain configurations from the config/chains directory."""
        chains_dir = 'config/chains'
        for filename in os.listdir(chains_dir):
            if filename.endswith('.yaml'):
                chain_name = filename.replace('.yaml', '')
                with open(os.path.join(chains_dir, filename), 'r') as f:
                    config = yaml.safe_load(f)
                    # Substitute environment variables in database config
                    if 'database' in config:
                        for key, value in config['database'].items():
                            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                                env_var = value[2:-1]
                                config['database'][key] = os.getenv(env_var)
                    self.chain_configs[chain_name] = config
                    
    def get_chain_config(self, chain_name: str) -> Dict[str, Any]:
        """Get configuration for a specific chain."""
        return self.chain_configs.get(chain_name)
    
    def get_enabled_plugins(self, chain_name: str) -> list:
        """Get list of enabled plugins for a chain."""
        chain_config = self.get_chain_config(chain_name)
        return chain_config.get('enabled_plugins', []) if chain_config else []
    
    def get_database_config(self, chain_name: str) -> Dict[str, Any]:
        """Get database configuration for a chain."""
        chain_config = self.get_chain_config(chain_name)
        return chain_config.get('database', {}) if chain_config else {} 
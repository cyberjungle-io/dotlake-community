import logging
import argparse
from src.core.config import ConfigLoader
from src.core.block_processor import BlockProcessor

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    parser = argparse.ArgumentParser(description='Blockchain Indexer')
    parser.add_argument('--chain', type=str, default='hydration',
                      help='Chain to index (default: hydration)')
    parser.add_argument('--end-block', type=int,
                      help='End block number (optional, for historical indexing)')
    args = parser.parse_args()

    setup_logging()
    logging.info(f"Starting indexer for chain: {args.chain}")

    try:
        # Load configuration
        config_loader = ConfigLoader()
        
        # Initialize block processor
        processor = BlockProcessor(args.chain, config_loader)
        
        # Start processing blocks
        processor.process_blocks(args.end_block)
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 
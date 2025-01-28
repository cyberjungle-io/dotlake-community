#!/bin/bash

# Create config directory structure
mkdir -p config/{chains,plugins/{common,hydration}}

# Copy template files
cp -r config-templates/* config/

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp config-templates/.env.template .env
    echo "Please update the .env file with your actual credentials"
fi

# Ensure .env file has correct permissions
chmod 600 .env

echo "Configuration templates have been copied to config/"
echo "Please update the following files with your specific settings:"
echo "1. .env file (update with your database credentials)"
echo "2. config/global.yaml"
echo "3. config/chains/hydration.yaml"
echo ""
echo "The following files can be used as-is or customized:"
echo "- config/plugins/common/transfers.yaml"
echo "- config/plugins/common/balances.yaml"
echo "- config/plugins/hydration/omnipool.yaml"
echo ""
echo "Remember: Both the config/ directory and .env file are git-ignored to protect your sensitive data"
echo "Make sure to update the .env file with your actual database credentials!" 
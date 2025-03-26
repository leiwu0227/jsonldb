"""
Dictionary operations with FolderDB.
Shows how to work with nested dictionaries and complex data structures.
"""

import os
import json
from datetime import datetime
from folderdb import FolderDB

def main():
    # Create a folder for our database
    db_folder = "dict_db"
    os.makedirs(db_folder, exist_ok=True)
    
    # Initialize the database
    db = FolderDB(db_folder)
    
    # Create some nested dictionaries
    products = {
        "prod1": {
            "name": "Gaming Laptop",
            "specs": {
                "cpu": "Intel i9",
                "ram": "32GB",
                "storage": "1TB SSD"
            },
            "price": 1999.99,
            "in_stock": True
        },
        "prod2": {
            "name": "Smartphone",
            "specs": {
                "cpu": "Snapdragon 8",
                "ram": "12GB",
                "storage": "256GB"
            },
            "price": 899.99,
            "in_stock": False
        }
    }
    
    inventory = {
        "loc1": {
            "name": "Main Warehouse",
            "items": {
                "prod1": 50,
                "prod2": 100
            },
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip": "10001"
            }
        },
        "loc2": {
            "name": "West Coast Hub",
            "items": {
                "prod1": 25,
                "prod2": 75
            },
            "address": {
                "street": "456 Tech Blvd",
                "city": "San Francisco",
                "zip": "94105"
            }
        }
    }
    
    # Save dictionaries to database
    db.upsert_dicts({
        "products": products,
        "inventory": inventory
    })
    
    # Update product price
    updates = {
        "prod1": {
            "price": 1899.99,  # Price reduction
            "in_stock": True
        }
    }
    db.upsert_dict("products", updates)
    
    # Query specific records
    some_products = db.get_dict(["products"], lower_key="prod1", upper_key="prod1")
    print("\nProduct details:")
    print(json.dumps(some_products["products"], indent=2))
    
    # Update inventory
    inventory_updates = {
        "loc1": {
            "items": {
                "prod1": 45  # 5 units sold
            }
        }
    }
    db.upsert_dict("inventory", inventory_updates)
    
    # Query all inventory
    all_inventory = db.get_dict(["inventory"])
    print("\nCurrent inventory:")
    print(json.dumps(all_inventory["inventory"], indent=2))
    
    # Delete a location
    db.delete_file("inventory", ["loc2"])
    
    # Show final state
    print("\nFinal database state:")
    print(str(db))
    
    # Cleanup
    for file in os.listdir(db_folder):
        os.remove(os.path.join(db_folder, file))
    os.rmdir(db_folder)

if __name__ == "__main__":
    main() 
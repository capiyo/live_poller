#!/usr/bin/env python3
"""
MongoDB Crypto Hack Cleanup Script
Removes attacker-injected crypto payment fields from ALL collections
"""

import os
import re
import sys
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Load environment variables
load_dotenv()

# Attacker's injected field names to remove
FIELDS_TO_REMOVE = [
    "crypto_btc",
    "crypto_discount", 
    "crypto_eth",
    "crypto_trc20",
    "payment_crypto",
    "payment_message",
    "payment_method",
    "card_available_in",
    "card_payment_status",
    "crypto_discount_active",
    "crypto_discount_percent",
    "notice_display",
    "payment_alert",
    "primary_payment_method",
    "urgent_message"
]

# All collections to clean
COLLECTIONS = [
    "comments",
    "comrades", 
    "events",
    "fcm_tokens",
    "fixtures",
    "games",
    "likes",
    "lineups",
    "posts",
    "room",
    "statistics",
    "sub_fixture_votes",
    "sub_fixtures",
    "timeline",
    "transactions",
    "user_archive_activities",
    "user_profiles",
    "users",
    "votes"
]

def get_mongo_connection():
    """Establish MongoDB connection from .env variables"""
    # Try different possible .env variable names
    mongo_uri = (
        os.getenv("MONGO_URI") or 
        os.getenv("MONGODB_URI") or
        os.getenv("DATABASE_URL") or
        os.getenv("DB_URI")
    )
    
    mongo_host = os.getenv("MONGO_HOST", "localhost")
    mongo_port = int(os.getenv("MONGO_PORT", "27017"))
    mongo_user = os.getenv("MONGO_USER")
    mongo_password = os.getenv("MONGO_PASSWORD")
    mongo_db = os.getenv("MONGO_DB") or os.getenv("DATABASE_NAME", "admin")
    
    if mongo_uri:
        print(f"✓ Using MongoDB URI connection")
        return MongoClient(mongo_uri), mongo_db
    
    # Build connection string from individual params
    if mongo_user and mongo_password:
        mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}"
    else:
        mongo_uri = f"mongodb://{mongo_host}:{mongo_port}/{mongo_db}"
    
    print(f"✓ Connecting to MongoDB at {mongo_host}:{mongo_port}")
    return MongoClient(mongo_uri), mongo_db

def clean_collection(db, collection_name, dry_run=True):
    """Remove attacker fields from a single collection"""
    collection = db[collection_name]
    
    # Find documents with any of the malicious fields
    query = {"$or": [{field: {"$exists": True}} for field in FIELDS_TO_REMOVE]}
    
    affected_docs = list(collection.find(query, {"_id": 1}))
    count = len(affected_docs)
    
    if count == 0:
        print(f"  ⚪ {collection_name}: No infected documents found")
        return 0
    
    print(f"  🔴 {collection_name}: Found {count} infected document(s)")
    
    if dry_run:
        # Just report, don't modify
        for doc in affected_docs[:3]:  # Show first 3 as sample
            print(f"     - Document ID: {doc['_id']}")
        if count > 3:
            print(f"     ... and {count - 3} more")
    else:
        # Perform the cleanup
        unset_statement = {field: "" for field in FIELDS_TO_REMOVE}
        result = collection.update_many(query, {"$unset": unset_statement})
        print(f"     ✓ Cleaned {result.modified_count} document(s)")
    
    return count

def main():
    print("=" * 60)
    print("🔐 MongoDB Crypto Hack Cleanup Script")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check for dry-run mode
    dry_run = "--dry-run" in sys.argv or "-d" in sys.argv
    force = "--force" in sys.argv or "-f" in sys.argv
    
    if dry_run:
        print("\n⚠️  DRY RUN MODE - No changes will be made")
    elif not force:
        print("\n⚠️  This will MODIFY your database!")
        response = input("Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("❌ Aborted.")
            sys.exit(0)
    
    # Connect to MongoDB
    try:
        client, db_name = get_mongo_connection()
        db = client[db_name]
        
        # Test connection
        client.admin.command('ping')
        print(f"✓ Successfully connected to database: {db_name}")
        
    except ConnectionFailure as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        print("\nPlease check your .env file has one of:")
        print("  - MONGO_URI=mongodb://user:pass@host:port/db")
        print("  - or MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_DB")
        sys.exit(1)
    
    # Show collections that actually exist
    existing_collections = db.list_collection_names()
    collections_to_clean = [c for c in COLLECTIONS if c in existing_collections]
    missing_collections = [c for c in COLLECTIONS if c not in existing_collections]
    
    print(f"\n📁 Found {len(existing_collections)} total collections")
    if missing_collections:
        print(f"⚠️  Missing collections (skipping): {', '.join(missing_collections)}")
    
    # Clean each collection
    print("\n🔍 Scanning collections for injected fields...\n")
    total_infected = 0
    
    for collection_name in collections_to_clean:
        infected_count = clean_collection(db, collection_name, dry_run)
        total_infected += infected_count
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"Collections scanned: {len(collections_to_clean)}")
    print(f"Total infected documents found: {total_infected}")
    
    if dry_run and total_infected > 0:
        print("\n💡 To apply changes, run:")
        print("   python clean_mongodb.py --force")
    elif not dry_run and total_infected > 0:
        print("\n✅ Cleanup completed successfully!")
        print("\n🔐 NEXT STEPS:")
        print("   1. Change MongoDB password immediately")
        print("   2. Check for unauthorized users: db.getUsers()")
        print("   3. Review audit logs for other suspicious activity")
        print("   4. Restart your application")
    
    if total_infected == 0:
        print("\n✓ No infected documents found - your database appears clean!")
    
    client.close()

if __name__ == "__main__":
    main()
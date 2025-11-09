#!/usr/bin/env python3
"""
MongoDB to SQLite Migration Script
Migrates users and data from MongoDB to SQLite database
"""

import os
import sys

def migrate_from_mongodb():
    """Migrate all data from MongoDB to SQLite"""
    try:
        # Import pymongo for MongoDB connection
        try:
            from pymongo import MongoClient
        except ImportError:
            print("❌ ERROR: pymongo not installed!")
            print("Install it with: pip install pymongo")
            return False
        
        from database_sqlite import DatabaseManager
        from datetime import datetime
        
        # Get MongoDB connection details
        mongodb_uri = os.getenv("MONGODB_URI")
        if not mongodb_uri:
            print("❌ ERROR: MONGODB_URI environment variable not set!")
            print("Please set your MongoDB connection string in Secrets/Environment Variables")
            return False
        
        print("=" * 60)
        print("MongoDB to SQLite Migration Tool")
        print("=" * 60)
        print(f"\n📊 Connecting to MongoDB...")
        
        # Connect to MongoDB
        mongo_client = MongoClient(mongodb_uri)
        
        # Try to get database from URI, or use default database name
        try:
            mongo_db = mongo_client.get_database()
        except:
            # If no database in URI, try common database names
            db_name = os.getenv("MONGODB_DATABASE", "telegram_bot")
            print(f"ℹ️  No database in URI, trying database: {db_name}")
            mongo_db = mongo_client[db_name]
        
        # Test connection
        mongo_client.server_info()
        print(f"✅ Connected to MongoDB successfully")
        
        # Initialize SQLite database
        print(f"\n📊 Connecting to SQLite...")
        sqlite_db = DatabaseManager()
        print(f"✅ Connected to SQLite: {sqlite_db.db_path}")
        
        # Statistics
        stats = {
            'users': 0,
            'admins': 0,
            'daily_usage': 0,
            'broadcasts': 0,
            'ad_sessions': 0,
            'ad_verifications': 0,
            'errors': 0
        }
        
        # Migrate Users
        print(f"\n" + "=" * 60)
        print("Migrating Users Collection")
        print("=" * 60)
        
        try:
            users_collection = mongo_db['users']
            users = list(users_collection.find())
            print(f"Found {len(users)} users in MongoDB")
            
            for user in users:
                try:
                    user_id = user.get('user_id')
                    if not user_id:
                        continue
                    
                    # Add user to SQLite
                    sqlite_db.add_user(
                        user_id=user_id,
                        username=user.get('username'),
                        first_name=user.get('first_name'),
                        last_name=user.get('last_name'),
                        user_type=user.get('user_type', 'free')
                    )
                    
                    # Set additional user data
                    if user.get('subscription_end'):
                        sqlite_db.set_premium(
                            user_id=user_id,
                            expiry_datetime=user.get('subscription_end'),
                            source=user.get('premium_source', 'paid')
                        )
                    
                    if user.get('is_banned'):
                        sqlite_db.ban_user(user_id)
                    
                    if user.get('session_string'):
                        sqlite_db.set_user_session(user_id, user.get('session_string'))
                    
                    if user.get('custom_thumbnail'):
                        sqlite_db.set_custom_thumbnail(user_id, user.get('custom_thumbnail'))
                    
                    if user.get('ad_downloads', 0) > 0:
                        sqlite_db.add_ad_downloads(user_id, user.get('ad_downloads'))
                    
                    stats['users'] += 1
                    print(f"✅ Migrated user: {user_id} (@{user.get('username', 'N/A')})")
                    
                except Exception as e:
                    print(f"⚠️  Error migrating user {user.get('user_id')}: {e}")
                    stats['errors'] += 1
            
        except Exception as e:
            print(f"❌ Error accessing users collection: {e}")
        
        # Migrate Admins
        print(f"\n" + "=" * 60)
        print("Migrating Admins Collection")
        print("=" * 60)
        
        try:
            admins_collection = mongo_db['admins']
            admins = list(admins_collection.find())
            print(f"Found {len(admins)} admins in MongoDB")
            
            for admin in admins:
                try:
                    user_id = admin.get('user_id')
                    if user_id:
                        sqlite_db.add_admin(
                            user_id=user_id,
                            added_by=admin.get('added_by', user_id)
                        )
                        stats['admins'] += 1
                        print(f"✅ Migrated admin: {user_id}")
                except Exception as e:
                    print(f"⚠️  Error migrating admin {admin.get('user_id')}: {e}")
                    stats['errors'] += 1
        
        except Exception as e:
            print(f"❌ Error accessing admins collection: {e}")
        
        # Migrate Daily Usage
        print(f"\n" + "=" * 60)
        print("Migrating Daily Usage Collection")
        print("=" * 60)
        
        try:
            daily_usage_collection = mongo_db['daily_usage']
            daily_usage = list(daily_usage_collection.find())
            print(f"Found {len(daily_usage)} daily usage records in MongoDB")
            
            # Note: SQLite will create new daily usage records as users download files
            # Old daily usage data is less critical, so we skip it
            print("ℹ️  Skipping old daily usage data (will be recreated as users use bot)")
            
        except Exception as e:
            print(f"ℹ️  Daily usage collection not found or error: {e}")
        
        # Close MongoDB connection
        mongo_client.close()
        
        # Print summary
        print(f"\n" + "=" * 60)
        print("Migration Summary")
        print("=" * 60)
        print(f"✅ Users migrated: {stats['users']}")
        print(f"✅ Admins migrated: {stats['admins']}")
        print(f"⚠️  Errors encountered: {stats['errors']}")
        print(f"\n✨ Migration completed successfully!")
        print(f"📁 SQLite database: {sqlite_db.db_path}")
        print(f"\n💡 Tip: Your SQLite database is now ready to use!")
        print(f"💡 You can safely remove MONGODB_URI from your environment variables")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def export_mongodb_to_json():
    """Export MongoDB data to JSON file for backup"""
    try:
        from pymongo import MongoClient
        import json
        
        mongodb_uri = os.getenv("MONGODB_URI")
        if not mongodb_uri:
            print("❌ ERROR: MONGODB_URI not set!")
            return False
        
        print("Connecting to MongoDB...")
        mongo_client = MongoClient(mongodb_uri)
        mongo_db = mongo_client.get_database()
        
        export_data = {}
        
        # Export each collection
        collections = ['users', 'admins', 'daily_usage', 'broadcasts', 'ad_sessions', 'ad_verifications']
        
        for collection_name in collections:
            try:
                collection = mongo_db[collection_name]
                docs = list(collection.find())
                
                # Convert ObjectId to string
                for doc in docs:
                    if '_id' in doc:
                        doc['_id'] = str(doc['_id'])
                
                export_data[collection_name] = docs
                print(f"✅ Exported {len(docs)} documents from {collection_name}")
            except Exception as e:
                print(f"⚠️  Error exporting {collection_name}: {e}")
        
        # Save to JSON file
        output_file = "mongodb_export.json"
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        print(f"\n✅ MongoDB data exported to: {output_file}")
        mongo_client.close()
        return True
        
    except ImportError:
        print("❌ pymongo not installed. Install with: pip install pymongo")
        return False
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("MongoDB to SQLite Migration Utility")
    print("=" * 60)
    print("\nOptions:")
    print("1. Migrate from MongoDB to SQLite")
    print("2. Export MongoDB to JSON (backup)")
    print("3. Exit")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == "1":
        print("\n⚠️  WARNING: This will import MongoDB users into SQLite")
        print("Make sure you have set MONGODB_URI in your environment variables")
        confirm = input("\nProceed with migration? (yes/no): ").lower()
        
        if confirm == "yes":
            migrate_from_mongodb()
        else:
            print("Migration cancelled")
    
    elif choice == "2":
        export_mongodb_to_json()
    
    elif choice == "3":
        print("Goodbye!")
    
    else:
        print("Invalid choice!")

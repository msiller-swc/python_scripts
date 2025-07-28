import requests
from datetime import datetime

# Metabase credentials and details
METABASE_URL = "https://insight.skilledsystems.io" 
USERNAME = "mariadelarosa@skilledwoundcare.com"
PASSWORD = ""
COLLECTION_ID = 242  # ✅ Replace with your collection ID

def get_session_token():
    """Authenticate and return a session token"""
    response = requests.post(f"{METABASE_URL}/api/session", json={
        "username": USERNAME,
        "password": PASSWORD
    })
    response.raise_for_status()
    return response.json()['id']

def get_collection_items(session_token, collection_id):
    """Get all items in a specific collection"""
    headers = {"X-Metabase-Session": session_token}
    response = requests.get(f"{METABASE_URL}/api/collection/{collection_id}/items", headers=headers)
    response.raise_for_status()
    return response.json()

def get_all_collections_recursive(session_token, collection_id):
    """Recursively get all collections and subcollections starting from a given collection"""
    collections_to_process = [collection_id]
    all_collections = []
    
    while collections_to_process:
        current_collection_id = collections_to_process.pop(0)
        all_collections.append(current_collection_id)
        
        try:
            items = get_collection_items(session_token, current_collection_id)['data']
            
            # Find subcollections and add them to the processing queue
            for item in items:
                if item['model'] == 'collection':
                    subcollection_id = item['id']
                    collections_to_process.append(subcollection_id)
                    print(f"📁 Found subcollection: {item['name']} (ID: {subcollection_id})")
        
        except Exception as e:
            print(f"⚠️ Error accessing collection {current_collection_id}: {e}")
            continue
    
    return all_collections

def get_card(session_token, card_id):
    """Fetch card metadata"""
    headers = {"X-Metabase-Session": session_token}
    response = requests.get(f"{METABASE_URL}/api/card/{card_id}", headers=headers)
    response.raise_for_status()
    return response.json()

def get_user(session_token, user_id):
    """Get user details by ID"""
    headers = {"X-Metabase-Session": session_token}
    response = requests.get(f"{METABASE_URL}/api/user/{user_id}", headers=headers)
    response.raise_for_status()
    return response.json()

def update_card(session_token, card_id, card_data, new_query):
    """Update the card's SQL query"""
    headers = {"X-Metabase-Session": session_token}
    payload = {
        "name": card_data["name"],
        "dataset_query": {
            "type": "native",
            "native": {
                "query": new_query,
                "template-tags": card_data['dataset_query']['native'].get("template-tags", {})
            },
            "database": card_data['database_id']
        },
        "display": card_data.get("display", "table")
    }
    response = requests.put(f"{METABASE_URL}/api/card/{card_id}", headers=headers, json=payload)
    response.raise_for_status()

def main():
    try:
        session_token = get_session_token()
        print("✅ Authenticated.")

        # Get all collections recursively
        print(f"🔍 Discovering all collections starting from collection {COLLECTION_ID}...")
        all_collections = get_all_collections_recursive(session_token, COLLECTION_ID)
        print(f"📁 Found {len(all_collections)} collections to process: {all_collections}")

        updated_count = 0
        total_cards_processed = 0

        # Process each collection
        for collection_id in all_collections:
            print(f"\n📂 Processing collection {collection_id}...")
            
            try:
                items = get_collection_items(session_token, collection_id)['data']
                cards_in_collection = [item for item in items if item['model'] == 'card']
                print(f"📄 Found {len(cards_in_collection)} cards in collection {collection_id}.")
                
                for item in cards_in_collection:
                    card_id = item['id']
                    card_name = item['name']
                    total_cards_processed += 1
                    print(f"\n🔍 Checking card: {card_name} (ID: {card_id}) in collection {collection_id}")

                    card_data = get_card(session_token, card_id)

                    if card_data.get('dataset_query', {}).get('type') != 'native':
                        print("⏭️  Not a native SQL card. Skipping.")
                        continue

                    query = card_data['dataset_query']['native']['query']

                    if "HISTORY:" in query:
                        print("✅ 'HISTORY:' already present. Skipping.")
                        continue

                    # Get creator info
                    creator_id = card_data.get("creator_id")
                    created_at = card_data.get("created_at")

                    creator_name = "Unknown"
                    created_date = "Unknown"

                    if creator_id:
                        try:
                            user_data = get_user(session_token, creator_id)
                            creator_name = user_data.get("common_name", user_data.get("email", "Unknown"))
                        except Exception as e:
                            print(f"⚠️ Could not fetch user {creator_id}: {e}")

                    if created_at:
                        created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime("%Y%m%d")

                    # Build dynamic header
                    dynamic_header = f"""-- *************************
--  PURPOSE:    <Question purpose>
--  NOTES:      <Relevant notes about the question, such as those
--              that would be helpful to developers who will see this.>
--  ------------------------
--  HISTORY:
--  --------
--  YYYYMMDD | <Developer name/initials> | <Optional Jira issue number> | <Description of updates.>
--  {created_date} | {creator_name} | <Optional Jira issue number> | Created.
-- *************************

"""

                    # Combine and update
                    new_query = dynamic_header + query
                    update_card(session_token, card_id, card_data, new_query)
                    print("✏️  Updated with new header.")
                    updated_count += 1
                    
            except Exception as e:
                print(f"⚠️ Error processing collection {collection_id}: {e}")
                continue

        print(f"\n✅ Done. Processed {total_cards_processed} total cards across {len(all_collections)} collections.")
        print(f"✏️  Updated {updated_count} cards with new headers.")

    except requests.exceptions.RequestException as e:
        print("❌ Request error:", e)
    except KeyError as e:
        print("❌ Data format error:", e)

if __name__ == "__main__":
    main()

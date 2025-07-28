import requests
from datetime import datetime

# Metabase credentials and details
METABASE_URL = "https://insight.skilledsystems.io" 
USERNAME = "mariadelarosa@skilledwoundcare.com"
PASSWORD = ""
COLLECTION_ID = 220  # ✅ Replace with your collection ID

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

        items = get_collection_items(session_token, COLLECTION_ID)['data']
        print(f"📁 Found {len(items)} items in collection {COLLECTION_ID}.")

        updated_count = 0

        for item in items:
            if item['model'] != 'card':
                continue  # Skip dashboards or pulses

            card_id = item['id']
            card_name = item['name']
            print(f"\n🔍 Checking card: {card_name} (ID: {card_id})")

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
--  JIRA TICKET: 
--  PURPOSE:    
--  NOTES:      
--  ------------------------
--  HISTORY:
--  {created_date} | {creator_name} | Created

-- *************************

"""

            # Combine and update
            new_query = dynamic_header + query
            update_card(session_token, card_id, card_data, new_query)
            print("✏️  Updated with new header.")
            print("📄 Final query:\n", new_query)
            updated_count += 1

        print(f"\n✅ Done. {updated_count} cards updated.")

    except requests.exceptions.RequestException as e:
        print("❌ Request error:", e)
    except KeyError as e:
        print("❌ Data format error:", e)

if __name__ == "__main__":
    main()

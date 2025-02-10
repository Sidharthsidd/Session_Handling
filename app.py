import pymongo
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, auth
from pymongo import MongoClient
from datetime import datetime

# Firebase Authentication Setup
cred = credentials.Certificate("foodapp-aa753-firebase-adminsdk-ey3ee-77962ef353.json")
firebase_admin.initialize_app(cred)

# MongoDB Connection Setup
DB_USER = "sidharthee1905"
DB_PASSWORD = "foodappserver"
DB_NAME = "test"
COLLECTION_SESSION = "session_email"
COLLECTION_CARTS = "carts"
COLLECTION_CLEANED_CARTS = "cleaned_carts"
COLLECTION_MENUS = "menus"
COLLECTION_SUGGESTED = "suggested_items"

connection_string = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.av3yj.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client[DB_NAME]

# Flask Setup
app = Flask(_name_)
CORS(app)

@app.route('/', methods=['POST'])
def get_user_email():
    try:
        id_token = request.json.get('id_token')
        decoded_token = auth.verify_id_token(id_token)
        email = decoded_token.get('email')
        
        if email:
            db[COLLECTION_SESSION].insert_one({"email": email, "created_at": datetime.utcnow()})
            return jsonify({"message": "Email stored successfully", "email": email}), 200
        else:
            return jsonify({"error": "Email not found in token"}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 400

db[COLLECTION_SESSION].create_index("created_at", expireAfterSeconds=600)

# Data Cleaning Function
def clean_data(data):
    return {
        "name": data.get("name", "Unknown Product"),
        "email": data.get("email", "Unknown Email")
    }

def process_new_data():
    with db[COLLECTION_CARTS].watch() as stream:
        for change in stream:
            full_document = change.get("fullDocument")
            if full_document:
                cleaned_data = clean_data(full_document)
                db[COLLECTION_CLEANED_CARTS].insert_one(cleaned_data)
                print(f"Cleaned data inserted: {cleaned_data}")

def store_recommendations():
    session = db[COLLECTION_SESSION].find_one(sort=[("_id", -1)])
    if not session:
        print("No session found")
        return
    
    email = session.get("email")
    carts = list(db[COLLECTION_CLEANED_CARTS].find({}))
    
    if not carts:
        print("No cart items found")
        return

    user_item_matrix = {}
    for item in carts:
        user_item_matrix.setdefault(item['email'], {})[item['name']] = 1

    user_item_df = pd.DataFrame.from_dict(user_item_matrix, orient='index').fillna(0)
    item_similarity_df = pd.DataFrame(cosine_similarity(user_item_df.T), index=user_item_df.columns, columns=user_item_df.columns)
    user_cart_items = [item.get('name') for item in db[COLLECTION_CLEANED_CARTS].find({"email": email})]

    recommendations = {}
    for item in user_cart_items:
        if item in item_similarity_df.index:
            similar_items_sorted = item_similarity_df[item].sort_values(ascending=False)
            for similar_item, similarity in similar_items_sorted.items():
                if similar_item not in user_cart_items and similar_item not in recommendations:
                    recommendations[similar_item] = similarity

    sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
    recommended_items = []
    for item, similarity in sorted_recommendations[:5]:
        menu_item = db[COLLECTION_MENUS].find_one({"name": item})
        if menu_item:
            recommended_items.append({
                "name": menu_item.get("name"),
                "recipe": menu_item.get("recipe"),
                "image": menu_item.get("image"),
                "category": menu_item.get("category"),
                "price": menu_item.get("price"),
                "similarity": similarity,
                "email": email
            })
    
    db[COLLECTION_SUGGESTED].delete_many({"email": email})
    if recommended_items:
        db[COLLECTION_SUGGESTED].insert_many(recommended_items)
        print(f"Recommendations stored for {email}")
    else:
        print("No recommendations generated")

if _name_ == '_main_':
    print("Listening for changes in the 'carts' collection...")
    process_new_data()
    print("Generating recommendations...")
    store_recommendations()
    app.run(debug=True)

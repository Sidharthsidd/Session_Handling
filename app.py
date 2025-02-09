from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, auth
import pymongo
from datetime import datetime
import threading

# Initialize Firebase Admin SDK
cred = credentials.Certificate("file.json")
firebase_admin.initialize_app(cred)

# MongoDB Atlas connection setup
DB_USER = "sidharthee1905"
DB_PASSWORD = "foodappserver"
DB_NAME = "test"

connection_string = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.av3yj.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_string)
db = client[DB_NAME]

# Collections
session_collection = db["session_email"]
cart_collection = db["carts"]
cleaned_cart_collection = db["cleaned_carts"]

# Flask setup
app = Flask(__name__)
CORS(app)

@app.route('/', methods=['POST'])
def store_user_email():
    try:
        id_token = request.json.get('id_token')
        decoded_token = auth.verify_id_token(id_token)
        email = decoded_token.get('email')
        
        if email:
            session_collection.insert_one({"email": email, "created_at": datetime.utcnow()})
            return jsonify({"message": "Email stored successfully", "email": email}), 200
        else:
            return jsonify({"error": "Email not found in token"}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Expire sessions after 600 seconds
session_collection.create_index("created_at", expireAfterSeconds=600)

# Function to clean cart data
def clean_data(data):
    return {
        "name": data.get("name", "Unknown Product"),
        "email": data.get("email", "Unknown Email")
    }

# Function to watch and process new cart data
def process_new_data():
    print("Listening for changes in 'carts' collection...")
    with cart_collection.watch() as stream:
        for change in stream:
            full_document = change.get("fullDocument")
            if full_document:
                cleaned_data = clean_data(full_document)
                cleaned_cart_collection.insert_one(cleaned_data)
                print(f"Cleaned data inserted: {cleaned_data}")

# Run cart data processing in a separate thread
listener_thread = threading.Thread(target=process_new_data, daemon=True)
listener_thread.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

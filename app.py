import pymongo
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, auth
from pymongo import MongoClient
from datetime import datetime
import threading

# Initialize Firebase Admin SDK
cred = credentials.Certificate("file.json")
firebase_admin.initialize_app(cred)

# MongoDB Atlas Configuration
DB_USER = "sidharthee1905"
DB_PASSWORD = "foodappserver"
DB_NAME = "test"
CARTS_COLLECTION = "carts"
CLEANED_CARTS_COLLECTION = "cleaned_carts"
SESSION_COLLECTION = "session_email"

connection_string = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.av3yj.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client[DB_NAME]
cart_collection = db[CARTS_COLLECTION]
cleaned_cart_collection = db[CLEANED_CARTS_COLLECTION]
session_collection = db[SESSION_COLLECTION]

# Ensure session emails expire after 10 minutes
session_collection.create_index("created_at", expireAfterSeconds=600)

# Flask App Setup
app = Flask(__name__)
CORS(app)

def clean_data(data):
    """ Cleans cart data before inserting into cleaned_carts collection. """
    return {
        "name": data.get("name", "Unknown Product"),
        "email": data.get("email", "Unknown Email")
    }

def process_new_data():
    """ Listens for new data in 'carts' collection and cleans it before storing. """
    print("Listening for changes in the 'carts' collection...")
    with cart_collection.watch() as stream:
        for change in stream:
            full_document = change.get("fullDocument")
            if full_document:
                cleaned_data = clean_data(full_document)
                cleaned_cart_collection.insert_one(cleaned_data)
                print(f"Cleaned data inserted: {cleaned_data}")

@app.route('/', methods=['POST'])
def get_user_email():
    """ Receives Firebase ID token, verifies it, and stores user email in MongoDB. """
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

if __name__ == "__main__":
    # Run MongoDB change stream in a separate thread
    change_stream_thread = threading.Thread(target=process_new_data, daemon=True)
    change_stream_thread.start()

    # Run Flask app
    app.run(debug=True)

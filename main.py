from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, auth
from pymongo import MongoClient
from datetime import datetime, timedelta

# Initialize Firebase Admin SDK
cred = credentials.Certificate("foodapp-aa753-firebase-adminsdk-ey3ee-77962ef353.json")
firebase_admin.initialize_app(cred)

# MongoDB Atlas Configuration
DB_USER = "sidharthee1905"
DB_PASSWORD = "foodappserver"
DB_NAME = "test"
COLLECTION_NAME = "session_email"

connection_string = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@cluster0.av3yj.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['POST'])
def get_user_email():
    try:
        id_token = request.json.get('id_token')
        decoded_token = auth.verify_id_token(id_token)
        email = decoded_token.get('email')
        
        if email:
            # Store email in MongoDB with an expiration time of 10 minutes
            collection.insert_one({"email": email, "created_at": datetime.utcnow()})
            return jsonify({"message": "Email stored successfully", "email": email}), 200
        else:
            return jsonify({"error": "Email not found in token"}), 400
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Ensure documents are deleted after a specific time
collection.create_index("created_at", expireAfterSeconds=600)  # 10 minutes expiration


if __name__ == '__main__':
    app.run(debug=True)

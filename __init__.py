import logging
from flask import Flask,request, jsonify  
import azure.functions as func
import jwt
import datetime
import pyodbc

app = Flask(__name__)

SECRET_KEY = "1234567890"

connection_string = (
        'Driver=" ";'
        'Server=tcp:"";'
        'Database="";'
        'Uid="";'
        'Pwd="";'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )

# Create a connection
conn = pyodbc.connect(connection_string)
# Create a cursor from the connection
cursor = conn.cursor()

# Code from Azure Function
def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return func.WsgiMiddleware(app.wsgi_app).handle(req,context)

# code for Flask Application
@app.route('/generate-token', methods=['POST'])
def generate_token():
    try:
        data = request.json
        username = data.get('username')
        password = data.get('password')

        # Check if username and password are provided
        if not username or not password:
            return jsonify({"error": "Username and password are required"}), 400

        # Define the payload for the JWT token
        payload = {
            'username': username,
            'password': password,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)  
        }

        # Generate the JWT token
        token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')

        return jsonify({"token": token})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_to_sql():
    try:
        # Get the JWT token from the request headers
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({"error": "Authorization header missing"}), 401

        token = auth_header.replace('Bearer ', '')

        # Verify and decode the JWT token
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

        data_list = request.get_json()
        if not isinstance(data_list, list):
            return jsonify({'error': 'Payload must be a list of JSON objects.'}), 400

        rows_inserted = 0  
        for data in data_list:
            cid = data.get('cid')
            name = data.get('name')
            gender = data.get('gender')
            if not (cid and name and gender):
                return jsonify({'error': 'Each payload must contain cid, name, and gender.'}), 400
            cursor.execute("INSERT INTO customers (cid, name, gender) VALUES (?, ?, ?)", cid, name, gender)
            rows_inserted += cursor.rowcount  

        conn.commit()

        return jsonify({
            'message': f'{rows_inserted} row(s) successfully uploaded to SQL Server table.',
            'status_code': 200
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route for Pull Data From SQL Server table
@app.route('/pull', methods=['POST'])
def pull_from_sql():
    try:
        # Get the JWT token from the request headers
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({"error": "Authorization header missing"}), 401

        token = auth_header.replace('Bearer ', '')

        # Verify and decode the JWT token
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

        # Get the condition, columns, and table name from the payload
        data = request.get_json()
        condition = data.get('condition', '')
        columns = data.get('columns', '*')
        table_name = data.get('table_name')

        if not table_name:
            return jsonify({'error': 'Table name not provided in the payload.'}), 400

        if not columns:
            return jsonify({'error': 'Columns not provided in the payload.'}), 400

        if columns != '*':
            columns = ', '.join(columns)

        # Fetch data based on the provided condition, columns, and table name
        if not condition:
            query_condition = f"SELECT {columns} FROM {table_name}"
        else:
            query_condition = f"SELECT {columns} FROM {table_name} WHERE {condition}"

        cursor.execute(query_condition)
        data_list = cursor.fetchall()

        if not data_list:
            return jsonify({'message': 'No records found for the specified condition.'}), 200

        # Prepare the response data
        response_data = []
        for row in data_list:
            response_row = {}
            for col in cursor.description:
                response_row[col[0]] = getattr(row, col[0])
            response_data.append(response_row)

        return jsonify({
            'data': response_data,
            'message': f'{len(response_data)} row(s) successfully fetched from SQL Server table.',
            'status_code': 200
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

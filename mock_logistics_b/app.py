import json
from flask import Flask, jsonify, request

app = Flask(__name__)

# Load mock data
DATA_FILE = 'data.json'
with open(DATA_FILE, 'r') as f:
    MOCK_DATA = json.load(f)


# Mock endpoint for Partner B
@app.route('/logistics_b/deliveries', methods=['GET'])
def get_deliveries():
    site_id = request.args.get('siteId')

    if site_id:
        filtered_data = [
            item for item in MOCK_DATA
            if item.get('location', {}).get('site_ref') == site_id
        ]

        return jsonify(filtered_data)

    return jsonify(MOCK_DATA)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

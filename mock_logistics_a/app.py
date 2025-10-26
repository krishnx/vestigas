import json
import random
from flask import Flask, jsonify, request

app = Flask(__name__)

DATA_FILE = 'data.json'
with open(DATA_FILE, 'r') as f:
    MOCK_DATA = json.load(f)


@app.route('/logistics_a/deliveries', methods=['GET'])
def get_deliveries():
    site_id = request.args.get('siteId')

    if random.random() < 0.5:
        app.logger.warning("Simulating 503 Service Unavailable for Partner A.")
        return jsonify({"detail": "Service is temporarily unavailable (simulated 503)"}), 503

    if site_id:
        filtered_data = [item for item in MOCK_DATA if item.get('site_id') == site_id]
        return jsonify(filtered_data)

    return jsonify(MOCK_DATA)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

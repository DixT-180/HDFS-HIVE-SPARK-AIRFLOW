# # from flask import Flask, request, jsonify
# # import requests
# # import sys
# # sys.stdout.reconfigure(line_buffering=True)

# # app = Flask(__name__)


# # @app.route('/minio-event', methods=['POST'])
# # def handle_minio_event():
# #     print('########################')
# #     event_data = request.json
# #     print(event_data)
# #     app.logger.info(f"Received MinIO event: {event_data}")
# #     return jsonify(event_data), 200

# # if __name__ == "__main__":
# #     app.run(host="0.0.0.0", port=5000)




# import sys
# sys.stdout.reconfigure(line_buffering=True)

# from flask import Flask, request, jsonify
# import requests

# app = Flask(__name__)

# # Airflow REST API endpoint (adjust port if needed)
# AIRFLOW_API = "http://airflow-apiserver:8080/api/v1/dags/minio_file_printer/dagRuns"

# @app.route('/minio-event', methods=['POST'])
# def handle_minio_event():
#     try:
#         app.logger.info("######## Received a webhook ########")
#         event_data = request.json
#         app.logger.info(f"Received MinIO event: {event_data}")

#         # Extract file info
#         record = event_data['Records'][0]
#         bucket = record['s3']['bucket']['name']
#         key = record['s3']['object']['key']

#         payload = {
#             "conf": {
#                 "bucket": bucket,
#                 "key": key
#             }
#         }

#         response = requests.post(
#             AIRFLOW_API,
#             json=payload,
#             auth=("airflow", "airflow")
#         )

#         if response.status_code == 200:
#             return jsonify({"message": "DAG triggered", "airflow_response": response.json()}), 200
#         else:
#             app.logger.error(f"Failed to trigger DAG: {response.text}")
#             return jsonify({"error": "Failed to trigger DAG", "airflow_response": response.text}), 500

#     except Exception as e:
#         app.logger.error(f"Exception in handle_minio_event: {e}")
#         return jsonify({"error": str(e)}), 500




# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)



import requests
from flask import Flask, jsonify, request
import logging

app = Flask(__name__)

DAG_ID = "extract_people_from_minio"
AIRFLOW_API_URL = f"http://airflow-apiserver:8080/api/v1/dags/{DAG_ID}/dagRuns"

@app.route('/minio-event', methods=['POST'])
def handle_minio_event():
    event_data = request.json
    app.logger.info(f"Received MinIO event: {event_data}")

    try:
        filename = event_data['Records'][0]['s3']['object']['key']
        bucket_name = event_data['Records'][0]['s3']['bucket']['name']
    except (KeyError, IndexError):
        return jsonify({"error": "Invalid event data format"}), 400

    payload = {
        "conf": {
            "filename": filename,
            "bucket": bucket_name
        }
    }

    try:
        response = requests.post(
            AIRFLOW_API_URL,
            headers={"Content-Type": "application/json"},
            auth=('airflow', 'airflow'),
            json=payload
        )
        if response.status_code in (200, 201):
            return jsonify({"message": "DAG triggered successfully"}), 200
        else:
            app.logger.error(f"Airflow API error: {response.status_code} - {response.text}")
            return jsonify({"error": "Failed to trigger DAG", "details": response.text}), 500

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {e}")
        return jsonify({"error": "Connection to Airflow failed"}), 500

if __name__ == "__main__":
    app.logger.setLevel(logging.DEBUG)
    app.run(host="0.0.0.0", port=5000)

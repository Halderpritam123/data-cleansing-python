from flask import request, jsonify
from services.s3_service import S3Service
from filters.filter_engine import FilterEngine
from datetime import datetime

class FilterController:
    def __init__(self):
        self.s3_service = S3Service()

    def filter_data(self):
        try:
            payload = request.get_json()
            bucket = payload["bucket"]
            file_key = payload["file_key"]
            selected_columns = payload["selected_columns"]
            sql_condition = payload.get("filters", "")
            target_filename = payload["target_filename"]

            # Generate timestamp string
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            target_filename_with_ts = f"{target_filename}_{timestamp}"

            # Step 1: Read data
            df = self.s3_service.read_csv(bucket, file_key)

            # Step 2: Apply filters
            engine = FilterEngine(df)
            filtered_df, dropped_df = engine.apply_filter(selected_columns, sql_condition)

            # Step 3: Upload filtered and dropped files
            base_path = "processed"
            filtered_key = f"{base_path}/{target_filename_with_ts}.csv"
            dropped_key = f"{base_path}/{target_filename_with_ts}_dropped.csv"

            self.s3_service.upload_csv(filtered_df, bucket, filtered_key)
            self.s3_service.upload_csv(dropped_df, bucket, dropped_key)

            return jsonify({
                "filtered": {
                    "fileName": f"{target_filename_with_ts}.csv",
                    "filePath": filtered_key,
                    "bucket": bucket
                },
                "dropped": {
                    "fileName": f"{target_filename_with_ts}_dropped.csv",
                    "filePath": dropped_key,
                    "bucket": bucket
                }
            }), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500


import logging
import requests
from src.const.const import APP_NAME
class LoggingService:
    def __init__(self, audit_api_url_base: str):
        self.audit_api_url_base = audit_api_url_base
        self.app_name = APP_NAME

    def save_log(self,data):
        try:
            url = f"{self.audit_api_url_base}/audits"
            headers = {
            "Content-Type": "application/json"
            }
            data["appName"] = self.app_name
            response = requests.post(url, json=data, headers=headers)
            return response
        except Exception as e:
            logging.exception(f"[LoggingService - save_log] - Error: {e}")
            raise

from target_hyphen.client import HyphenSink

class PaymentsSink(HyphenSink):
    name = "payments"
    endpoint = "/payments"
    batch = None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        batch = record.pop("batch", None)

        if not self.batch:
            # Create the batch if not already created
            batch["applicationId"] = self.config.get("app_id")
            batch["partyId"] = self.config.get("party_id")
            batch["companyNumber"] = self.config.get("company_number", "0")
            response = self.request_api(
                "POST", endpoint="/batches", request_data=batch
            )

            if response.ok:
                self.logger.info(f"Successfully created batch")
                self.batch = response.json()
            elif response.json().get("error", "") == "Duplicate batch number":
                self.batch = response.json()
            else:
                self.logger.error(f"Failed to create batch: {response.text}")
                raise Exception(f"Failed to create batch: {response.text}")

        return {
            "id": record.pop("id", None),
            "applicationId": self.config.get("app_id"),
            "partyId": self.config.get("party_id"),
            "batchId": batch.get("batchId"),
            "payments": [record]
        }

    def upsert_record(self, record: dict, context: dict):
        state_updates = {}
        id = record.pop("id", None)

        try:
            # Create the payment
            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=[record]
            )

            if response.ok:
                self.logger.info(f"Successfully posted payment")
                return id, True, state_updates
            else:
                self.logger.error(f"Failed to post payment: {response.text}")
                return id, False, {"error": f"{response.status_code} - {response.text}"}
        except Exception as e:
            self.logger.error(f"Failed to post payment: {str(e)}")
            return id, False, {"error": str(e)}

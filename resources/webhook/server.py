"""
Workshop Lab 2: Real-Time Audit Trail - Operations Webhook Server

This Flask server acts as a centralized logging endpoint for Kafka infrastructure
governance. In a mature enterprise environment, any state-changing action
performed on the Kafka cluster must be recorded in an immutable audit trail.

Lab Workflow:
1. Kpow is configured with this server's URL (http://webhook-server:9000).
2. When a user performs an action in Kpow (like creating a topic or editing a config),
   Kpow sends a JSON payload to this server.
3. This server parses the "Audit Event" and prints it to the console,
   simulating a security monitor or Slack integration.

The payload contains critical metadata:
- The user who performed the action.
- The specific action taken (e.g., TOPIC_CREATE).
- The parameters of the change.
- A timestamp of the event.
"""

import json
from datetime import datetime

from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["POST"])
def webhook():
    """
    Primary endpoint for receiving Audit events from Kpow.
    Expects a POST request with a JSON body.
    """
    # Timestamp for the local console log
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n[{now}] --- AUDIT EVENT RECEIVED ---")

    # Kpow sends a 'Content-Type: application/json' header
    if request.is_json:
        try:
            # Extract the audit data
            data = request.get_json()

            # Print formatted JSON to the console for students to inspect.
            # In a real scenario, this might be sent to PagerDuty, Slack, or Splunk.
            print("Action Details:")
            print(json.dumps(data, indent=4))

            # Example: Extracting specific fields for high-visibility logging
            user = data.get("user", "Unknown User")
            action = data.get("action", "Unknown Action")
            print(f"SUMMARY: User '{user}' triggered action '{action}'")

        except Exception as e:
            print(f"ERROR: Failed to parse audit payload: {e}")
    else:
        # Security warning: non-JSON requests are unexpected
        print("⚠️ WARNING: Received non-JSON payload. Check Kpow Webhook configuration.")

    # Return 200 OK to Kpow to acknowledge receipt of the audit event
    return "Audit Log Recorded", 200


if __name__ == "__main__":
    # host="0.0.0.0" allows the container to be reachable within the Docker network
    # port=9000 matches the Kpow WEBHOOK_URL configuration
    print("Governance Webhook Server starting on port 9000...")
    print("Awaiting Kafka Audit events...")
    app.run(host="0.0.0.0", port=9000, debug=False)

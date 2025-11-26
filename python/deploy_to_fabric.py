
import os
import requests
from msal import ConfidentialClientApplication

# Environment variables
client_id = os.getenv("079b4b50-9290-4d34-b902-5adf4f460666")
client_secret = os.getenv("uq_8Q~dHp_saYppcy466t8uOnf2Rr3cOwZWYZcZ9")
tenant_id = os.getenv("0d1140e7-b7b2-4f63-9354-31e03923ad5d")
workspace_id = os.getenv("71c5be23-e608-4f87-922a-2f396880e42a")

authority = f"https://login.microsoftonline.com/{tenant_id}"
scope = ["https://analysis.windows.net/powerbi/api/.default"]

# Authenticate
app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
token = app.acquire_token_for_client(scopes=scope)
access_token = token["access_token"]

# Upload notebook example
headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"

payload = {
    "displayName": "My Notebook",
    "type": "notebook",
    "definition": {
        "format": "ipynb",
        "content": open("myfolder/ci/cd_workflow_test.Notebook/notebook-content.py").read()
    }
}

response = requests.post(url, headers=headers, json=payload)
print(response.status_code, response.text)

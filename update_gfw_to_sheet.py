import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo" 
SERVICE_ACCOUNT_FILE = "service_account.json" 

geometry = {
    "type": "Polygon",
    "coordinates": [[
        [110.15497, 0.67329],
        [110.38332, 0.67329],
        [110.38332, 0.91435],
        [110.15497, 0.91435],
        [110.15497, 0.67329]
    ]]
}

sql = """
SELECT longitude, latitude, wur_radd_alerts__date, wur_radd_alerts__confidence
FROM results
WHERE wur_radd_alerts__date >= '2025-07-01'
AND wur_radd_alerts__date <= '2025-10-01'
"""

url = "https://data-api.globalforestwatch.org/dataset/wur_radd_alerts/latest/query"
headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
body = {"geometry": geometry, "sql": sql}

resp = requests.post(url, headers=headers, json=body)
if resp.status_code != 200:
    print(f"Gagal mengambil data ({resp.status_code}): {resp.text}")
    exit()

data = resp.json()
if "data" not in data or len(data["data"]) == 0:
    print("Tidak ada data ditemukan untuk area/tanggal tersebut.")
    exit()

df = pd.DataFrame(data["data"])
print(f"Berhasil ambil {len(df)} data dari GFW.")

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
client = gspread.authorize(creds)

sheet = client.open_by_key(SPREADSHEET_ID).sheet1

sheet.clear()

sheet.update([df.columns.values.tolist()] + df.values.tolist())

print(f"Data berhasil dikirim ke Google Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")

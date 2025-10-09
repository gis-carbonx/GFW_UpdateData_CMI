import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime

API_KEY = "912b99d5-ecc2-47aa-86fe-1f986b9b070b"
SPREADSHEET_ID = "1UW3uOFcLr4AQFBp_VMbEXk37_Vb5DekHU-_9QSkskCo"

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

end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=7)

sql = f"""
SELECT longitude, latitude, wur_radd_alerts__date, wur_radd_alerts__confidence
FROM results
WHERE wur_radd_alerts__date >= '{start_date}'
AND wur_radd_alerts__date <= '{end_date}'
"""

url = "https://data-api.globalforestwatch.org/dataset/wur_radd_alerts/latest/query"
headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}
body = {
    "geometry": geometry,
    "sql": sql
}

print("Mengambil data GFW...")
response = requests.post(url, headers=headers, json=body)

if response.status_code != 200:
    print(f"Gagal mengambil data ({response.status_code}): {response.text}")
    exit()

data = response.json()

#Google Sheet
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID).sheet1

if "data" in data and len(data["data"]) > 0:
    df = pd.DataFrame(data["data"])

    df.rename(columns={
        "longitude": "Longitude",
        "latitude": "Latitude",
        "wur_radd_alerts__date": "Tanggal",
        "wur_radd_alerts__confidence": "Kepercayaan"
    }, inplace=True)


    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

    print(f"{len(df)} data berhasil dikirim ke Google Sheet ({start_date} → {end_date})")
else:
    print("Tidak ada data ditemukan untuk periode/area tersebut.")

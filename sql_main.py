import pyodbc
from pymongo import MongoClient
import time

# เริ่มจับเวลา
start_time = time.time()

# เชื่อมต่อ MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["TPSO_logs"]
mongo_collection = mongo_db["TPSO_Data_06-05-2025-14-16"]

# เชื่อมต่อ SQL Server
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LAPTOP-82Q11TTE\\SQLEXPRESS01;"
    "DATABASE=EstDb;"
    "Trusted_Connection=yes;"
)
sql_conn = pyodbc.connect(conn_str)
cursor = sql_conn.cursor()

print("Connected to MongoDB and SQL Server")

# ดึง MatNo ล่าสุดจาก tbMaterial ที่ขึ้นต้น MB
cursor.execute("""
    SELECT TOP 1 MatNo FROM dbo.tbMaterial
    WHERE MatNo LIKE 'MB%' AND ISNUMERIC(SUBSTRING(MatNo, 3, LEN(MatNo))) = 1
    ORDER BY CAST(SUBSTRING(MatNo, 3, LEN(MatNo)) AS INT) DESC
""")
last_row = cursor.fetchone()
matno_counter = int(last_row[0][2:]) + 1 if last_row else 1

# ตัวนับ
batch_size = 50
batch_number = 0
inserted_material = 0
inserted_price = 0
skipped_docs = 0
skipped_price_duplicates = 0

while True:
    docs = list(mongo_collection.find().skip(batch_number * batch_size).limit(batch_size))
    if not docs:
        break

    print(f"Processing batch {batch_number + 1}...")

    for doc in docs:
        try:
            item = doc.get('item', {})
            commodityCode = item.get('commodityCode', '')[:25]
            unitName = item.get('unitName', '')[:50]

            if not commodityCode or not unitName:
                print(f"Skipped: Missing commodityCode or unitName in doc {doc.get('_id')}")
                skipped_docs += 1
                continue

            # === material ===
            cursor.execute("SELECT MatNo FROM dbo.tbMaterial WHERE MatID = ?", (commodityCode,))
            row = cursor.fetchone()

            if row:
                matNo = row[0]
            else:
                matNo = f"MB{matno_counter:07d}"
                cursor.execute("""
                    INSERT INTO dbo.tbMaterial (MatNo, MatID, MatDesc, MatStatus, MatType)
                    VALUES (?, ?, ?, ?, ?)
                """, (matNo, commodityCode, '-', 1, 1))
                inserted_material += 1
                matno_counter += 1
                print(f"Inserted Material: {commodityCode} -> {matNo}")

            # === price ===
            provinceID = "10"
            brandID = 1
            pCreateby = "SAdmin"

            for year_data in item.get('years', []):
                year = str(year_data.get('year', ''))[:4]
                for month_data in year_data.get('months', []):
                    month = str(month_data.get('month', '')).zfill(2)[:2]
                    try:
                        priceCur = float(month_data.get('priceCur', 0))
                    except (ValueError, TypeError):
                        print(f"Skipped: Invalid priceCur in doc {doc.get('_id')}")
                        continue

                    # ตรวจสอบว่ามีข้อมูลซ้ำหรือยัง (MatNo, MonthID, PYear, ProvinceID, BrandID)
                    cursor.execute("""
                        SELECT COUNT(1) FROM dbo.tbPrice
                        WHERE MatNo = ? AND MonthID = ? AND PYear = ? AND ProvinceID = ? AND BrandID = ?
                    """, (matNo, month, year, provinceID, brandID))
                    exists = cursor.fetchone()[0]

                    if exists == 0:
                        cursor.execute("""
                            INSERT INTO dbo.tbPrice (MatNo, MatID, MonthID, PYear, ProvinceID, BrandID, PUnit, Price, PCreateby)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (matNo, commodityCode, month, year, provinceID, brandID, unitName, priceCur, pCreateby))
                        inserted_price += 1
                        print(f"Inserted Price: {commodityCode}, {month}/{year}, MatNo: {matNo}")
                    else:
                        skipped_price_duplicates += 1
                        print(f"Skipped duplicate Price: {commodityCode}, {month}/{year}, MatNo: {matNo}")

        except Exception as e:
            print(f"Error processing doc {doc.get('_id')}: {e}")
            continue

    batch_number += 1

# commit แล้วปิดการเชื่อมต่อ
sql_conn.commit()
sql_conn.close()
mongo_client.close()

# จับเวลาสิ้นสุด
end_time = time.time()

# สรุปผล
print("\n==== Summary ====")
print(f"Total batches processed: {batch_number}")
print(f"Total materials inserted: {inserted_material}")
print(f"Total price rows inserted: {inserted_price}")
print(f"Total price rows skipped (duplicates): {skipped_price_duplicates}")
print(f"Total skipped documents: {skipped_docs}")
print(f"Total time: {end_time - start_time:.2f} seconds")

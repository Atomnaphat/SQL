import pyodbc
from pymongo import MongoClient
import time
import uuid

# จับเวลาเริ่มต้น
start_time = time.time()

# เลือกโหมด
mode = input("เลือกโหมด: 1 = Update, Insert, No Action | 2 = Insert Only\n")

# เชื่อมต่อ MongoDB
try:
    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["TPSO_logs"]
    mongo_collection = mongo_db["TPSO_Data_26-05-2025-14-27"]
    print("Connected to MongoDB")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
    exit()

# สตริงการเชื่อมต่อ SQL Server
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LAPTOP-82Q11TTE\\SQLEXPRESS01;"
    "DATABASE=EstDb;"
    "Trusted_Connection=yes;"
)

# เชื่อมต่อ SQL Server
try:
    sql_conn = pyodbc.connect(connection_string)
    sql_cursor = sql_conn.cursor()
    print("Connected to SQL Server")
except Exception as e:
    print(f"Failed to connect to SQL Server: {e}")
    exit()

# ดึง MatNo ล่าสุดจาก tbPrice ที่ขึ้นต้นด้วย MB
sql_cursor.execute("""
    SELECT TOP 1 MatNo FROM dbo.tbPrice
    WHERE MatNo LIKE 'MB%' AND ISNUMERIC(SUBSTRING(MatNo, 3, LEN(MatNo))) = 1
    ORDER BY CAST(SUBSTRING(MatNo, 3, LEN(MatNo)) AS INT) DESC
""")
last_matno_row = sql_cursor.fetchone()
if last_matno_row:
    last_number = int(last_matno_row[0][2:])
else:
    last_number = 0

matno_counter = last_number + 1

# ตัวนับ
total_data = 0
updated_count = 0
inserted_count = 0

# ขนาด batch
batch_size = 50
batch_number = 0

while True:
    mongo_documents = list(mongo_collection.find().skip(batch_number * batch_size).limit(batch_size))
    if not mongo_documents:
        break

    print(f"Fetched batch {batch_number + 1}")

    for doc in mongo_documents:
        try:
            item = doc.get('item') or {}
            commodityCode = item.get('commodityCode', '')[:25]
            unitName = item.get('unitName', '')[:50]
            if not commodityCode or not unitName:
                print(f"Skipped: Missing commodityCode or unitName in doc {doc.get('_id')}")
                continue

            provinceID = "10"
            brandID = 1
            pCreateby = "SAdmin"

            years_data = item.get('years', [])
            for year_data in years_data:
                year = str(year_data.get('year', ''))[:4]
                months_data = year_data.get('months', [])
                for month_data in months_data:
                    month = str(month_data.get('month', '')).zfill(2)[:2]
                    try:
                        priceCur = float(month_data.get('priceCur', 0))
                    except (TypeError, ValueError):
                        print(f"Skipped: Invalid priceCur in doc {doc.get('_id')}")
                        continue

                    total_data += 1

                    sql_cursor.execute("""
                        SELECT Price 
                        FROM dbo.tbPrice
                        WHERE MatID = ? AND MonthID = ? AND PYear = ? AND ProvinceID = ? AND BrandID = ? AND PUnit = ?
                    """, (commodityCode, month, year, provinceID, brandID, unitName))

                    current_price = sql_cursor.fetchone()

                    if current_price:
                        if mode == '1':
                            if current_price[0] != priceCur:
                                sql_update = """
                                    UPDATE dbo.tbPrice
                                    SET Price = ?
                                    WHERE MatID = ? AND MonthID = ? AND PYear = ? AND ProvinceID = ? AND BrandID = ? AND PUnit = ?
                                """
                                sql_cursor.execute(sql_update, (priceCur, commodityCode, month, year, provinceID, brandID, unitName))
                                updated_count += 1
                                print(f"Updated: {commodityCode}, {month}, {year}")
                    else:
                        matNo = f"MB{matno_counter:07d}"
                        matno_counter += 1

                        sql_insert = """
                            INSERT INTO dbo.tbPrice (MatNo, MatID, MonthID, PYear, ProvinceID, BrandID, PUnit, Price, PCreateby)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        sql_cursor.execute(sql_insert, (matNo, commodityCode, month, year, provinceID, brandID, unitName, priceCur, pCreateby))
                        inserted_count += 1
                        print(f"Inserted: {commodityCode}, {month}, {year}, MatNo: {matNo}")

        except Exception as e:
            print(f"Error processing document {doc.get('_id')}: {e}")
            continue

    batch_number += 1

# Commit and close
sql_conn.commit()
mongo_client.close()
sql_conn.close()

# จับเวลาสิ้นสุด
end_time = time.time()

# สรุป
print("\n==== Summary ====")
print(f"Mode: {'Update + Insert' if mode == '1' else 'Insert Only'}")
print(f"Total processing time: {end_time - start_time:.2f} seconds")
print(f"Total data processed: {total_data}")
print(f"Updated data: {updated_count}")
print(f"Inserted data: {inserted_count}")

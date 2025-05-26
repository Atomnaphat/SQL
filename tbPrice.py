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

# ดึง MatNo ล่าสุดจาก tbPrice ที่ขึ้นต้นด้วย MB
sql_cursor.execute("""
    SELECT TOP 1 MatNo FROM dbo.tbPrice
    WHERE MatNo LIKE 'MB%' AND ISNUMERIC(SUBSTRING(MatNo, 3, LEN(MatNo))) = 1
    ORDER BY CAST(SUBSTRING(MatNo, 3, LEN(MatNo)) AS INT) DESC
""")
last_matno_row = sql_cursor.fetchone()
if last_matno_row:
    last_number = int(last_matno_row[0][2:])  # ตัด 'MB' แล้วแปลงเป็นตัวเลข
else:
    last_number = 0

matno_counter = last_number + 1  # เริ่มนับต่อจากตัวล่าสุด

# ตัวนับ
total_data = 0
updated_count = 0
inserted_count = 0

# ขนาดของแต่ละหน้า
batch_size = 50
batch_number = 0

while True:
    # ดึงข้อมูลจาก MongoDB ทีละ 50 เอกสาร
    mongo_documents = list(mongo_collection.find().skip(batch_number * batch_size).limit(batch_size))
    if not mongo_documents:
        break

    print(f"Fetched batch {batch_number + 1}")

    for doc in mongo_documents:
        commodityCode = doc['item']['commodityCode'][:25]
        unitName = doc['item']['unitName'][:50]

        provinceID = "10"
        brandID = 1
        pCreateby = "SAdmin"

        for year_data in doc['item']['years']:
            year = str(year_data['year'])[:4]
            for month_data in year_data['months']:
                month = str(month_data['month']).zfill(2)[:2]
                priceCur = month_data['priceCur']

                # นับข้อมูลรวม
                total_data += 1

                # ตรวจสอบราคาปัจจุบันก่อนที่จะอัปเดตหรือแทรก
                sql_cursor.execute("""
                    SELECT Price 
                    FROM dbo.tbPrice
                    WHERE MatID = ? AND MonthID = ? AND PYear = ? AND ProvinceID = ? AND BrandID = ? AND PUnit = ?
                """, (commodityCode, month, year, provinceID, brandID, unitName))

                current_price = sql_cursor.fetchone()

                if current_price:
                    if mode == '1':
                        # อัปเดตราคาเมื่อแถวที่ตรงตามเงื่อนไขมีอยู่และราคาต่างจากเดิม
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
                    # สร้าง MatNo ใหม่แบบต่อเนื่อง
                    matNo = f"MB{matno_counter:07d}"
                    matno_counter += 1

                    # แทรกข้อมูลใหม่หากไม่พบแถวที่ตรงตามเงื่อนไข
                    sql_insert = """
                        INSERT INTO dbo.tbPrice (MatNo, MatID, MonthID, PYear, ProvinceID, BrandID, PUnit, Price, PCreateby)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    sql_cursor.execute(sql_insert, (matNo, commodityCode, month, year, provinceID, brandID, unitName, priceCur, pCreateby))
                    inserted_count += 1
                    print(f"Inserted: {commodityCode}, {month}, {year}, MatNo: {matNo}")
    
    # เพิ่ม batch_number เพื่อตรวจสอบข้อมูลในชุดถัดไป
    batch_number += 1

# บันทึกการเปลี่ยนแปลงและปิดการเชื่อมต่อ
sql_conn.commit()
mongo_client.close()
sql_conn.close()

# จับเวลาสิ้นสุด
end_time = time.time()

# แสดงสรุปและเวลา
print(f"Total processing time: {end_time - start_time:.2f} seconds")
print(f"Total data processed: {total_data}")
print(f"Updated data: {updated_count}")
print(f"Inserted data: {inserted_count}")

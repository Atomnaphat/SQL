import pyodbc
from pymongo import MongoClient
import time

# จับเวลาเริ่มต้น
start_time = time.time()

# เชื่อมต่อ MongoDB
try:
    mongo_client = MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["TPSO_logs"]
    mongo_collection = mongo_db["TPSO_Data_06-05-2025-10-41"]
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

# ฟังก์ชันเพื่อหา MatNo สูงสุดและสร้างค่าใหม่ที่ต่อเนื่อง
def get_next_matno():
    sql_cursor.execute("SELECT ISNULL(MAX(CAST(SUBSTRING(MatNo, 3, LEN(MatNo) - 2) AS INT)), 0) FROM dbo.tbMaterial WHERE MatNo LIKE 'MB%'")
    max_no = sql_cursor.fetchone()[0]
    return f"MB{max_no + 1:07d}"

# ตัวนับ
total_data = 0
inserted_count = 0
inserted_items = []  # เก็บ MatID และ SC3No ที่ถูกแทรก

# ขนาดของแต่ละหน้า
batch_size = 50
batch_number = 0

while True:
    # ดึงข้อมูลจาก MongoDB ทีละ 20 เอกสาร
    mongo_documents = list(mongo_collection.find().skip(batch_number * batch_size).limit(batch_size))
    if not mongo_documents:
        break
    
    print(f"Fetched batch {batch_number + 1}")

    for doc in mongo_documents:
        commodityCode = doc['item']['commodityCode'][:25]
        commodityNameTH = doc['item']['commodityNameTH'][:400]
        unitName = doc['item']['unitName'][:255]

        # นับข้อมูลรวม
        total_data += 1
        
        # ดึงค่า MatNo ใหม่
        matNo = get_next_matno()
        createBy = "SAdmin"
        matStatus = 1

        # ตรวจสอบ 8 ตัวแรกของ commodityCode
        prefix = commodityCode[:8]

        # ตรวจสอบว่ามี SC3No ที่ตรงกับ prefix หรือไม่
        sql_cursor.execute("""
            SELECT SC3No
            FROM dbo.tbMaterial
            WHERE LEFT(MatID, 8) = ?
        """, (prefix,))
        
        result = sql_cursor.fetchone()
        if result:
            sc3No = result[0]
        else:
            sc3No = "NEW_CODE"  # กำหนดค่าตามต้องการ

        # ตรวจสอบว่ามีข้อมูลอยู่แล้วหรือไม่
        sql_cursor.execute("""
            SELECT COUNT(*)
            FROM dbo.tbMaterial
            WHERE MatID = ?
        """, (commodityCode,))

        (record_count,) = sql_cursor.fetchone()

        if record_count > 0:
            print(f"Material {commodityCode} already exists.")
        else:
            # แทรกข้อมูลใหม่
            try:
                sql_insert = """
                    INSERT INTO dbo.tbMaterial (MatNo, SC3No, MatID, MatName, Unit, Createby, MatStatus)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                sql_cursor.execute(sql_insert, (matNo, sc3No, commodityCode, commodityNameTH, unitName, createBy, matStatus))
                inserted_count += 1
                inserted_items.append((commodityCode, sc3No))  # เก็บทั้ง MatID และ SC3No
                print(f"Inserted: {commodityCode}")
            except pyodbc.IntegrityError as e:
                print(f"Failed to insert {commodityCode}: {e}")

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
print(f"Inserted data: {inserted_count}")

# แสดงรายการที่ถูกแทรก
print("Items inserted:")
for item in inserted_items:
    print(f"MatID: {item[0]}, SC3No: {item[1]}")
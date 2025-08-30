# E-commerce Data Pipeline ด้วย Apache Airflow & Docker  


โปรเจกต์นี้เป็นการจำลอง **Data Pipeline ของระบบ E-commerce** โดยทำงานตั้งแต่การ **Extract → Transform → Load (ETL)** จากข้อมูลดิบหลายรูปแบบ ไปจนถึงการโหลดเข้าสู่ **Data Warehouse (PostgreSQL)** พร้อมระบบ **แจ้งเตือนอัตโนมัติ** กรณีที่ Pipeline ล้มเหลว หรือพบความผิดปกติใน Server Logs  


## โปรเจกต์นี้ทำอะไรบ้าง?  
Pipeline นี้จัดการข้อมูลหลัก 3 ประเภท  
1. **Sales Data** – ข้อมูลการขาย เช่น ยอดขาย, จำนวนสินค้าที่ซื้อ  
2. **User Events Data** – พฤติกรรมผู้ใช้ เช่น การคลิก, การเข้าชม, การเพิ่มสินค้าในตะกร้า  
3. **Server Logs** – บันทึกการทำงานของระบบ  

### เป้าหมาย  
- ทำความสะอาดข้อมูล (Handle missing/invalid values)  
- แปลงข้อมูลให้อยู่ในรูปแบบที่พร้อมวิเคราะห์  
- โหลดข้อมูลเข้าฐานข้อมูล PostgreSQL เพื่อใช้ต่อกับ BI Tools  
- แจ้งเตือนอัตโนมัติ หาก Pipeline หรือ Server Logs พบความผิดปกติ  


## ไฮไลต์ของโปรเจกต์  
- **Setup ด้วย Docker Compose**: ใช้ไม่กี่คำสั่งก็ได้ Airflow + PostgreSQL + Redis พร้อมใช้งาน  
- **ETL Code แบ่งชัดเจน**: `extract.py`, `transform.py`, `load.py` → โครงสร้างอ่านง่าย 
- **Data Cleaning & Transformation**  
  - จัดการ Missing/Invalid values เช่น ราคาติดลบ, จำนวนสินค้าเป็นตัวอักษร  
  - แปลง Log ไฟล์ให้กลายเป็นตารางที่ Query ได้ง่าย  
- **ระบบแจ้งเตือนอัตโนมัติ**  
  - แจ้งเตือน DE ทางอีเมลเมื่อ Pipeline Task ล้มเหลว  
  - แจ้งเตือนทีม Dev หากพบ Error Logs เกินกำหนด  
- **XCom Communication**: ส่งข้อมูลระหว่าง Tasks ใน Airflow อย่างมีประสิทธิภาพ  


## เทคโนโลยีที่ใช้  
- **Apache Airflow** – Workflow Orchestration  
- **Docker / Docker Compose** – Environment Setup  
- **PostgreSQL** – Data Warehouse + Airflow Metadata DB  
- **Redis** – Message Broker สำหรับ Airflow Workers  
- **Python (Pandas)** – Data Transformation  
- **SMTP (Gmail)** – Email Alert  


## โครงสร้างโปรเจกต์  
```plaintext
├── dags/
│   ├── e_commerce_etl_pipeline.py    # Airflow DAG หลัก
│   └── db_connection_test_dag.py     # DAG สำหรับทดสอบ DB Connection
├── src/
│   ├── extract.py                    # Extract Data
│   ├── transform.py                  # Transform Data
│   ├── load.py                       # Load Data
│   └── generate_data.py              # Mock Data Generator
    └── db_connection_test           
├── data/                             # Mock Data
│   ├── sale_data.csv
│   ├── events.json
│   └── server.log
├── .env.example
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## วิธีใช้งาน
- **ติดตั้ง Docker + Docker Compose และ Python 3.8+**
- **สร้าง .env** โดยคัดลอกจากไฟล์ **.env.example** และกรอกข้อมูลที่จำเป็นลงไป
- สร้าง Mock data ด้วยคำสั่ง
  ```plaintext
  python generate_data.py
  ```
- รันทั้งหมดด้วย **Docker**
  ```plaintext
  docker compose up -d --build
  ```
- เข้า Airflow UI ผ่าน ``` http://localhost:8080 ```

## วิธีทดสอบ Pipeline
- Query Data ใน PostgreSQL
- Trigger DAG ผ่าน Airflow UI เพื่อทดสอบ Email Alert (อาจลองสร้าง error เพื่อทดสอบระบบแจ้งเตือน)

## สิ่งที่ได้เรียนรู้จากโปรเจกต์นี้
1. การออกแบบและพัฒนา ETL/ELT Pipeline ที่รองรับข้อมูลหลายรูปแบบ
2. การทำ Data Cleaning & Transformation ให้พร้อมใช้งานจริง
3. การใช้ Apache Airflow เพื่อจัดการ Workflow และตั้ง Scheduling
4. การจัดการ Error Handling & Alert System
5. การออกแบบระบบโดยคิดถึง End-user (เช่น Data Analyst)

## สิ่งที่อยากพัฒนาต่อ (Future Work)
1. เพิ่มการจำกัดสิทธิ์เข้าถึงข้อมูล ตามทีม (เช่น DA, Sales, Dev)
2. รองรับ แหล่งข้อมูลภายนอก เช่น API
3. เชื่อมต่อกับ BI Tools เพื่อสร้าง Dashboard
4. เพิ่มระบบ ตรวจสอบคุณภาพข้อมูล (Data Quality Checks) อัตโนมัติ

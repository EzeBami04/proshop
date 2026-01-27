import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv
import logging

load_dotenv()
#======================= config  ===========================
logging.basicConfig(level=logging.INFO)

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

#============================================================

#============ Convert dictionaries to Dataframe ============

def dataframe(lst_dict: dict):
    df = pd.DataFrame(lst_dict)
    return df

def connect_local():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-N6PGLK1\\MSSQLSERVER01;"
            "DATABASE=tsvmap_msp;"
            "Trusted_Connection=yes;"
        )
        return conn
    except Exception as e:
        logging.error(f"cannot connect to loal instance {e}")

def connect_to_db():
    logging.info("Connecting to database")
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
        logging.info("Succefully Connected  to Database")
        return conn
    except Exception as e:
        logging.warning(f"Error connecting to database: {e}")

def to_python(val):
    if pd.isna(val):
        return None
    if isinstance(val, (pd.Timestamp,)):
        return val.to_pydatetime()
    if hasattr(val, "item"): 
        return val.item()
    return val

def load_wrk_orders(df, conn):
    if df.empty:
        return
    columns = df.columns.tolist()
    upsert_values = ", ".join(["?"] * len(columns))
    source_cols = ", ".join(columns)
    pk = "workOrderNumber"
    
    insert_col = ", ".join([f"source.{col}" for col in columns])
    update_cols = [c for c in columns if c != pk]
    update_clause = ",\n  ".join([f"{c} = source.{c}" for c in update_cols])
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        create_table_sql = """
        IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'wrk_orders'
        AND s.name = 'stg'
        )
        BEGIN
            CREATE TABLE stg.wrk_orders(
            workOrderNumber VARCHAR(20) PRIMARY KEY,
            scheduledStartDate VARCHAR(50),
            scheduledEndDate VARCHAR(50),
            dueDate VARCHAR(50),
            customerPlainText VARCHAR(255),
            client_po_num VARCHAR(50),
           
            quantityOrdered INT,
            qtyComplete INT,
            qtyInWIP INT,
            qtyShipped INT,
            qtyNotYetShipped INT,

            dateShipped VARCHAR(50),
            daysToShip INT,
            status VARCHAR(50),

            hoursCurrentTarget VARCHAR(50),
            hoursTotalSpent VARCHAR(50),

            setupTimeHoursActualLabel VARCHAR(50),
            setupTimeHoursPlannedTarget VARCHAR(50),
            setupTimeHoursPlannedVarianceLabor VARCHAR(50),

            runningTimeHoursActualLabor VARCHAR(50),
            runningTimeHoursPlannedTargetLabor VARCHAR(50),
            runningTimeHoursPlannedVarianceLabor VARCHAR(50),

            laborWIP FLOAT,
            standardizedLaborClass VARCHAR(50),
            standardizedLaborRate VARCHAR(50),

            partPlainText VARCHAR(MAX),
            partRev VARCHAR(50),
            pfiPrice VARCHAR(50),
            assemblyClass VARCHAR(50),
            btiPrice VARCHAR(50),
            countAsOnTime BIT,

            totalCappedWIP FLOAT,
            totalUncappedWIP FLOAT,
            estWODollarAmount FLOAT,
            type VARCHAR(50),

            wipCogsLabor FLOAT,
            wipCogsMaterials FLOAT,
            wipDirectOverhead FLOAT,
            wipIndirectOverhead FLOAT
        );

        END;

        """
        cursor.execute(create_table_sql)
        conn.commit()
        # ================ Load   =====================================

        merge_sql = f"""
        MERGE stg.wrk_orders AS target
        USING (VALUES ({upsert_values}))
        AS source ({source_cols})
        ON target.{pk} = source.{pk}
        WHEN MATCHED THEN
            UPDATE SET 
                {update_clause}
        WHEN NOT MATCHED THEN
            INSERT VALUES ({insert_col});
        """

        #======= insert records ====================
        records = []
        for row in df.itertuples(index=False, name=None):
            clean_row = []
            for v in row:
                if pd.isna(v):
                    clean_row.append(None)
                elif isinstance(v, float) and v.is_integer():
                    clean_row.append(int(v))
                elif hasattr(v, "item"):  # numpy types
                    clean_row.append(v.item())
                else:
                    clean_row.append(v)
            records.append(tuple(clean_row))


        cursor.executemany(merge_sql, records)
        conn.commit()

        logging.info("Work orders successfully inserted/updated")
    except Exception as e:
        conn.rollback()
        logging.warning(f"Database Error: {e}")
    finally:
        cursor.close()
        conn.close()

def load_invoice(df, conn):
    # from invoice import get_invoice_details
    # df = get_invoice_details(token)
    if df.empty:
        return
    
    records = list(df.itertuples(index=False, name=None))

    #========= Create Invoice Table ==============
    stg_invoice = """
        IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'invoice'
        AND s.name = 'stg'
        )
        BEGIN
            CREATE TABLE stg.invoice(
                inv_date VARCHAR(45),
                inv_id VARCHAR(20) PRIMARY KEY,
                client_po_num VARCHAR(45),
                client_part_num VARCHAR(45),
                client_id Varchar(10),
                ship_to_address varchar(255),
                ship_to_city varchar(100),
                amount FLOAT,
                status VARCHAR(45)
            );
        END;
;"""
    
    upsert_stmt = """
        MERGE stg.invoice AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source
            (inv_date, inv_id,  client_po_num, client_part_num, client_id, ship_to_address,
            ship_to_city, amount, status)
        ON target.inv_id = source.inv_id
        WHEN MATCHED THEN
            UPDATE SET
                inv_date = source.inv_date,
                client_po_num = source.client_po_num,
                client_part_num = source.client_part_num,
                client_id = source.client_id,
                ship_to_address = source.ship_to_address,
                ship_to_city = source.ship_to_city,
                amount = source.amount,
                status = source.status
        WHEN NOT MATCHED THEN
            INSERT (inv_date, inv_id,  client_po_num, client_part_num, client_id, ship_to_address, 
                    ship_to_city, amount, status)
            VALUES (
                source.inv_date,
                source.inv_id,
                source.client_po_num,
                source.client_part_num,
                source.client_id,
                source.ship_to_address,
                source.ship_to_city,
                source.amount,
                source.status
            );

        """
    try:
        with conn.cursor() as cursor:
            cursor.execute(stg_invoice)
            cursor.executemany(upsert_stmt, records)

        conn.commit()
        logging.info("Invoice successfully inserted/updated")
    except Exception as e:
        logging.warning(f"Database error {e}")
        # conn.rollback()

def load_part(df, conn):
    if df.empty:
        logging.info("No parts to load")
        return

    records = list(df.itertuples(index=False, name=None))

    # Complete CREATE TABLE statement
    create_table_sql = """
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'parts'
        AND s.name = 'stg'
    )
    BEGIN
        CREATE TABLE stg.parts(
            unique_id VARCHAR(8) PRIMARY KEY,
            partNumber VARCHAR(MAX) NOT NULL,
            partName VARCHAR(MAX),
            client_part_num VARCHAR(MAX),
            qtyInWip FLOAT,
            part_rev VARCHAR(255),
            inventory_account VARCHAR(255),
            inventory_import_value VARCHAR(255),
            inventory_qty VARCHAR(255),
            last_activity_date VARCHAR(50),
            least_amount_to_order VARCHAR(50),
            legacy_id VARCHAR(50),
            minimum_order_qty VARCHAR(50),
            minimum_qty_on_hand VARCHAR(50),
            min_reorder_point VARCHAR(50),
            multiplier_markup VARCHAR(50),
            net_inspect_import_notes VARCHAR(MAX),
            original_sort_position VARCHAR(50),
            packaging_instructions VARCHAR(MAX),
            pricing_notes VARCHAR(MAX),
            sales_account VARCHAR(255),
            shipping_cost VARCHAR(50),
            standardized_labor_class VARCHAR(50),
            template_group VARCHAR(50),
            universal VARCHAR(50),
            status VARCHAR(255),
            notes VARCHAR(MAX),
            leadTime VARCHAR(50)
        );
    END;
    """

    # Merge / Upsert statement
    upsert_sql = """
    MERGE stg.parts AS target
    USING (
        SELECT
            CAST(? AS VARCHAR(8)) AS unique_id,
            CAST(? AS VARCHAR(MAX)) AS partNumber,
            CAST(? AS VARCHAR(MAX)) AS partName,
            CAST(? AS VARCHAR(MAX)) AS client_part_num,
            CAST(? AS FLOAT) AS qtyInWip,
            CAST(? AS VARCHAR(255)) AS part_rev,
            CAST(? AS VARCHAR(255)) AS inventory_account,
            CAST(? AS VARCHAR(255)) AS inventory_import_value,
            CAST(? AS VARCHAR(255)) AS inventory_qty,
            CAST(? AS VARCHAR(50)) AS last_activity_date,
            CAST(? AS VARCHAR(50)) AS least_amount_to_order,
            CAST(? AS VARCHAR(50)) AS legacy_id,
            CAST(? AS VARCHAR(50)) AS minimum_order_qty,
            CAST(? AS VARCHAR(50)) AS minimum_qty_on_hand,
            CAST(? AS VARCHAR(50)) AS min_reorder_point,
            CAST(? AS VARCHAR(50)) AS multiplier_markup,
            CAST(? AS VARCHAR(MAX)) AS net_inspect_import_notes,
            CAST(? AS VARCHAR(50)) AS original_sort_position,
            CAST(? AS VARCHAR(MAX)) AS packaging_instructions,
            CAST(? AS VARCHAR(MAX)) AS pricing_notes,
            CAST(? AS VARCHAR(255)) AS sales_account,
            CAST(? AS VARCHAR(50)) AS shipping_cost,
            CAST(? AS VARCHAR(50)) AS standardized_labor_class,
            CAST(? AS VARCHAR(50)) AS template_group,
            CAST(? AS VARCHAR(50)) AS universal,
            CAST(? AS VARCHAR(255)) AS status,
            CAST(? AS VARCHAR(MAX)) AS notes,
            CAST(? AS VARCHAR(50)) AS leadTime
    ) AS source
    ON target.partNumber = source.partNumber
    WHEN MATCHED THEN
        UPDATE SET
            unique_id = source.unique_id,
            partName = source.partName,
            client_part_num = source.client_part_num,
            qtyInWip = source.qtyInWip,
            part_rev = source.part_rev,
            inventory_account = source.inventory_account,
            inventory_import_value = source.inventory_import_value,
            inventory_qty = source.inventory_qty,
            last_activity_date = source.last_activity_date,
            least_amount_to_order = source.least_amount_to_order,
            legacy_id = source.legacy_id,
            minimum_order_qty = source.minimum_order_qty,
            minimum_qty_on_hand = source.minimum_qty_on_hand,
            min_reorder_point = source.min_reorder_point,
            multiplier_markup = source.multiplier_markup,
            net_inspect_import_notes = source.net_inspect_import_notes,
            original_sort_position = source.original_sort_position,
            packaging_instructions = source.packaging_instructions,
            pricing_notes = source.pricing_notes,
            sales_account = source.sales_account,
            shipping_cost = source.shipping_cost,
            standardized_labor_class = source.standardized_labor_class,
            template_group = source.template_group,
            universal = source.universal,
            status = source.status,
            notes = source.notes,
            leadTime = source.leadTime
    WHEN NOT MATCHED THEN
        INSERT (unique_id, partNumber, partName, client_part_num, qtyInWip, part_rev, inventory_account,
                inventory_import_value, inventory_qty, last_activity_date, least_amount_to_order, legacy_id,
                minimum_order_qty, minimum_qty_on_hand, min_reorder_point, multiplier_markup,
                net_inspect_import_notes, original_sort_position, packaging_instructions, pricing_notes,
                sales_account, shipping_cost, standardized_labor_class, template_group, universal,
                status, notes, leadTime)
        VALUES (source.unique_id, source.partNumber, source.partName, source.client_part_num, source.qtyInWip,
                source.part_rev, source.inventory_account, source.inventory_import_value, source.inventory_qty,
                source.last_activity_date, source.least_amount_to_order, source.legacy_id, source.minimum_order_qty,
                source.minimum_qty_on_hand, source.min_reorder_point, source.multiplier_markup,
                source.net_inspect_import_notes, source.original_sort_position, source.packaging_instructions,
                source.pricing_notes, source.sales_account, source.shipping_cost, source.standardized_labor_class,
                source.template_group, source.universal, source.status, source.notes, source.leadTime);
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.executemany(upsert_sql, records)
            conn.commit()
        logging.info("Parts successfully inserted/updated")
    except Exception as e:
        logging.warning(f"Database error {e}")
        conn.rollback()


def load_client_po(df, conn):
    if df.empty:
        return
    records = list(df.itertuples(index=False, name=None))

    create_table_sql = """
        IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'client_po'
        AND s.name = 'stg'
        )
        BEGIN
            CREATE TABLE stg.client_po(
                client_po_num VARCHAR(50) PRIMARY KEY,
                client_plain_text VARCHAR(255),
                total_amount FLOAT
            );
        END;
        """
    upsert_stmt = """
        MERGE stg.client_po AS target
        USING (VALUES (?, ?, ?)) AS source
            (client_po_num, client_plain_text, total_amount)
        ON target.client_po_num = source.client_po_num
        WHEN MATCHED THEN
            UPDATE SET
                client_plain_text = source.client_plain_text,
                total_amount = source.total_amount
        WHEN NOT MATCHED THEN
            INSERT (client_po_num, client_plain_text, total_amount)
            VALUES (
                source.client_po_num,
                source.client_plain_text,
                source.total_amount
            );
        """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

            cursor.executemany(upsert_stmt, records)
            conn.commit()
        logging.info("Client PO successfully inserted/updated")
    except Exception as e:
        logging.warning(f"Database error {e}")
        conn.rollback()
    finally:
        conn.close()

def load_contacts(df, conn):
    if df.empty:
        return
    records = list(df.itertuples(index=False, name=None))
    create_table = """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = 'contacts'
            AND s.name = 'stg'
            )
            Begin
                CREATE TABLE stg.contacts(
                    created_time VARCHAR(50),
                    name VARCHAR(255),
                    company_name VARCHAR(255),
                    main_contact VARCHAR(255),
                    contact_email VARCHAR(255) PRIMARY KEY,
                    phone_number VARCHAR(50),
                    preferred_status VARCHAR(50),
                    previous_contact_code VARCHAR(50),
                    customer_supplier_code VARCHAR(50),
                    previous_name VARCHAR(255),
                    type VARCHAR(50),
                    payment_terms VARCHAR(50),
                    price_code VARCHAR(50),
                    project_code_on_ps VARCHAR(50),
                    bill_to_address VARCHAR(255),
                    bill_to_city VARCHAR(100),
                    bill_to_state VARCHAR(100),
                    bill_to_zipcode VARCHAR(20),
                    website_address VARCHAR(255),
                    status VARCHAR(50)
                );
            END;
    """
    upsert_stmt = f"""
        MERGE stg.contacts AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source
            (created_time, name, company_name, main_contact, contact_email, phone_number, 
            preferred_status, previous_contact_code, customer_supplier_code, previous_name, type, 
            payment_terms, price_code, project_code_on_ps, bill_to_address, bill_to_city, bill_to_state,
            bill_to_zipcode, website_address, status)
        ON target.contact_email = source.contact_email
        WHEN MATCHED THEN
            UPDATE SET
                created_time = source.created_time,
                name = source.name,
                company_name = source.company_name,
                main_contact = source.main_contact,
                phone_number = source.phone_number,
                preferred_status = source.preferred_status,
                previous_contact_code = source.previous_contact_code,
                customer_supplier_code = source.customer_supplier_code,
                previous_name = source.previous_name,
                type = source.type,
                payment_terms = source.payment_terms,
                price_code = source.price_code,
                project_code_on_ps = source.project_code_on_ps,
                bill_to_address = source.bill_to_address,
                bill_to_city = source.bill_to_city,
                bill_to_state = source.bill_to_state,
                bill_to_zipcode = source.bill_to_zipcode,
                website_address = source.website_address,
                status = source.status
        WHEN NOT MATCHED THEN
            INSERT (
                created_time, name, company_name, main_contact, contact_email, phone_number,
                preferred_status, previous_contact_code, customer_supplier_code, previous_name, type, 
                payment_terms, price_code, project_code_on_ps, bill_to_address, bill_to_city, bill_to_state,
                bill_to_zipcode, website_address, status
            )
            VALUES (
                source.created_time,
                source.name,
                source.company_name,
                source.main_contact,
                source.contact_email,
                source.phone_number,
                source.preferred_status,
                source.previous_contact_code,
                source.customer_supplier_code,
                source.previous_name,
                source.type,
                source.payment_terms,
                source.price_code,
                source.project_code_on_ps,
                source.bill_to_address,
                source.bill_to_city,
                source.bill_to_state,
                source.bill_to_zipcode,
                source.website_address,
                source.status
            );
            """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table)
            conn.commit()
            cursor.executemany(upsert_stmt, records)
            conn.commit()
        logging.info("Contacts successfully inserted/updated")
    except Exception as e:
        logging.warning(f"Database error {e}")
        conn.rollback()
    finally:
        conn.close()

def load_equipments(df, conn):
    """load equipment module into the staging  database"""
    if df.empty:
        return
    records = list(df.itertuples(index=False, name=None))
    create_table = """
        IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'equipments'
        AND s.name = 'stg'
        )
        BEGIN
            CREATE TABLE stg.equipments(
                equipment_number VARCHAR(50) PRIMARY KEY,
                equipment_type VARCHAR(max),
                serial_number VARCHAR(max),
                legacy_id Varchar(50),
                tool Varchar(Max),
                tool_name Varchar(max),
                created_at Varchar(50),
                modified_at Varchar(50),
                location VARCHAR(max),
                status VARCHAR(50)
            );
        END;"""

    upsert = """
        MERGE stg.equipments As target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source
            (equipment_number, equipment_type, serial_number, 
                legacy_id, tool, tool_name, created_at, modified_at, location, status)
        ON target.equipment_number = source.equipment_number
        WHEN MATCHED THEN
            UPDATE SET
                equipment_number = source.equipment_number,
                equipment_type = source.equipment_type,
                serial_number = source.serial_number,
                location = source.location,
                legacy_id = source.legacy_id,
                tool = source.tool,
                tool_name = source.tool_name,
                created_at = source.created_at,
                modified_at = source.modified_at,
                status = source.status
        WHEN NOT MATCHED  THEN
            INSERT (equipment_number, equipment_type, serial_number, legacy_id, tool, tool_name, created_at, modified_at,
                    location, status)
            VALUES (
                source.equipment_number,
                source.equipment_type,
                source.serial_number,
                source.legacy_id,
                source.tool,
                source.tool_name,
                source.created_at,
                source.modified_at,
                source.location,
                source.status
            );"""
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table)
            conn.commit()
            cursor.executemany(upsert, records)
            conn.commit()
        logging.info("Equipments successfully inserted/updated")
    except Exception as e:
        logging.warning(f"Database error {e}")
        conn.rollback()
    finally:
        conn.close()



def load_bills(df, conn):
    if df.empty:
        logging.info("No bills to load")
        return

    columns = df.columns.tolist()
    placeholders = ", ".join(["?"] * len(columns))
    source_cols = ", ".join(columns)

    pk = "bill_id"
    insert_cols = ", ".join(columns)
    update_cols = [c for c in columns if c != pk]
    update_clause = ", ".join([f"{c} = source.{c}" for c in update_cols])

    records = list(df.itertuples(index=False, name=None))

    create_table_sql = """
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'bill'
        AND s.name = 'stg'
    )
    BEGIN
        CREATE TABLE stg.bill(
            bill_id VARCHAR(25) PRIMARY KEY,
            date_issued VARCHAR(50),
            due_date VARCHAR(50),
            status VARCHAR(25),
            reference_num VARCHAR(50),
            supplier_id VARCHAR(50),
            supplierPlainText VARCHAR(255),
            supplierAddress VARCHAR(255),
            supplierCity VARCHAR(100),
            supplierZipCode VARCHAR(100),
            totalDollars FLOAT,
            paymentTerms VARCHAR(50),
            paymentTermsDiscount VARCHAR(50),
            paymentTermsDiscountDays VARCHAR(50)
        );
    END;
    """

    merge_sql = f"""
    MERGE stg.bill AS target
    USING (VALUES ({placeholders})) AS source ({source_cols})
    ON target.bill_id = source.bill_id
    WHEN MATCHED THEN
        UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_cols});
    """

    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        cursor.execute(create_table_sql)
        conn.commit()

        
        cursor.executemany(merge_sql, records)
        conn.commit()
        

        logging.info("Bills successfully inserted/updated")

    except Exception as e:
        conn.rollback()
        logging.error(f"Database Error: {e}")

    finally:
        cursor.close()
        conn.close()
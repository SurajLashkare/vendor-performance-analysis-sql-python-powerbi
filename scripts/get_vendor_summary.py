import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from ingestion_db import ingest_db

# --------------------------------------------------
# LOGGING CONFIG
# --------------------------------------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# --------------------------------------------------
# FUNCTION: CREATE VENDOR SUMMARY (READ → psycopg2)
# --------------------------------------------------
def create_vendor_summary(conn):
    """
    Merge different tables to get overall vendor summary
    """

    query = """
    WITH FreightSummary AS (
        SELECT
            "VendorNumber",
            SUM("Freight") AS "FreightCost"
        FROM vendor_invoice
        GROUP BY "VendorNumber"
    ),

    PurchaseSummary AS (
        SELECT
            p."VendorNumber",
            p."VendorName",
            p."Brand",
            p."Description",
            p."PurchasePrice",
            pp."Price" AS "ActualPrice",
            pp."Volume",
            SUM(p."Quantity") AS "TotalPurchaseQuantity",
            SUM(p."Dollars") AS "TotalPurchaseDollars"
        FROM purchases p
        JOIN purchase_prices pp
            ON p."Brand" = pp."Brand"
        WHERE p."PurchasePrice" > 0
        GROUP BY
            p."VendorNumber",
            p."VendorName",
            p."Brand",
            p."Description",
            p."PurchasePrice",
            pp."Price",
            pp."Volume"
    ),

    SalesSummary AS (
        SELECT
            "VendorNo",
            "Brand",
            SUM("SalesQuantity") AS "TotalSalesQuantity",
            SUM("SalesDollars") AS "TotalSalesDollars",
            SUM("SalesPrice") AS "TotalSalesPrice",
            SUM("ExciseTax") AS "TotalExciseTax"
        FROM sales
        GROUP BY "VendorNo", "Brand"
    )

    SELECT
        ps."VendorNumber",
        ps."VendorName",
        ps."Brand",
        ps."Description",
        ps."PurchasePrice",
        ps."ActualPrice",
        ps."Volume",
        ps."TotalPurchaseQuantity",
        ps."TotalPurchaseDollars",
        ss."TotalSalesQuantity",
        ss."TotalSalesDollars",
        ss."TotalSalesPrice",
        ss."TotalExciseTax",
        fs."FreightCost"
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps."VendorNumber" = ss."VendorNo"
        AND ps."Brand" = ss."Brand"
    LEFT JOIN FreightSummary fs
        ON ps."VendorNumber" = fs."VendorNumber"
    ORDER BY ps."TotalPurchaseDollars" DESC
    """

    return pd.read_sql_query(query, conn)


# --------------------------------------------------
# FUNCTION: CLEAN DATA
# --------------------------------------------------
def clean_data(df):
    """Clean and enrich vendor summary data"""

    # datatype conversions
    df["Volume"] = df["Volume"].astype("float64")
    df["TotalPurchaseQuantity"] = df["TotalPurchaseQuantity"].astype("int64")

    # fill missing values
    df.fillna(0, inplace=True)

    # clean string columns
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    # feature engineering
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
    df["ProfitMargin"] = (df["GrossProfit"] / df["TotalSalesDollars"]) * 100
    df["StockTurnover"] = df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"]
    df["SalesToPurchaseRatio"] = (
        df["TotalSalesDollars"] / df["TotalPurchaseDollars"]
    )

    return df


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":

    # ----------------------------------------
    # POSTGRES CONNECTION (READ)
    # ----------------------------------------
    conn = psycopg2.connect(
        host="localhost",
        database="inventory",
        user="postgres",
        password="root",
        port=5432
    )

    # ----------------------------------------
    # SQLALCHEMY ENGINE (WRITE)
    # ----------------------------------------
    engine = create_engine(
        "postgresql+psycopg2://postgres:root@localhost:5432/inventory"
    )

    try:
        logging.info("Creating Vendor Summary...")

        summary_df = create_vendor_summary(conn)
        logging.info(summary_df.head())

        logging.info("Cleaning Data...")
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head())

        logging.info("Ingesting Data into PostgreSQL...")
        ingest_db(clean_df, "vendor_sales_summary", engine)

        logging.info("Process Completed Successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

    finally:
        conn.close()

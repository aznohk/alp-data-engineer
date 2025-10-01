from databaseConfig import get_db_session
from sqlalchemy import text
from transformDataTransaction import get_data_criteria
from getDataBronze import getDataNasabahRaw, getDataTransactionRaw
from getDataSilver import getDataCriteria
from transformDataSilver import transformDataSilver, inserDataTransaction
from transformDataGold import transformDataGold, insertDataGold
from pipeline_orchestrator import PipelineOrchestrator
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def my_database_connection(db_session):
    if not db_session:
        print("No Database available")
        return
    
    try:
        result = db_session.execute(text("SELECT version();")).scalar()
        print(f"Database version: {result}")
        print("Database successfully connection")
    except Exception as e:
        print(f"Error during database connection: {e}")
    finally:
        pass


def run_legacy_pipeline():
    """Run the original bronze to silver pipeline (legacy mode)"""
    start_time = time.time()
    db_session = get_db_session()
    if db_session:
        try:
            my_database_connection(db_session)

            list_nasabah_raw = getDataNasabahRaw('bronze', 'data_nasabah_raw')
            list_trx_raw = getDataTransactionRaw('bronze', 'transactions_raw')
            criterias = getDataCriteria('silver', 'criteria')
            if list_nasabah_raw is not None and list_trx_raw is not None and criterias is not None :
                list_trx_transform = transformDataSilver(list_trx=list_trx_raw, list_nasabah=list_nasabah_raw, list_criteria=criterias)
                data_inserted = inserDataTransaction(list_trx=list_trx_transform)
                if not data_inserted.empty:
                    print(f"Successfully processed and inserted {len(data_inserted)} transactions to database")
                else:
                    print(f"No new data to insert")
            else:
                print("\nFailed to get DataFrame. Check for errors.")

        finally:
            db_session.close()
            print("Database session closed")
            end_time = time.time()
            duration = end_time - start_time
            print(f"Execution time : {duration:.4f} seconds")
    else:
        print("Could not get a database session. Exiting")


def run_complete_pipeline():
    """Run the complete bronze to silver to gold pipeline"""
    logger.info("🚀 Starting Complete Data Pipeline...")
    orchestrator = PipelineOrchestrator()
    success = orchestrator.run_complete_pipeline()
    
    if success:
        logger.info("🎯 Complete pipeline finished successfully!")
        return True
    else:
        logger.error("💥 Complete pipeline failed!")
        return False


def main():
    """Main function with pipeline selection"""
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        mode = "complete"  # Default to complete pipeline
    
    if mode == "legacy":
        print("🔄 Running Legacy Pipeline (Bronze -> Silver)")
        run_legacy_pipeline()
    elif mode == "complete":
        print("🔄 Running Complete Pipeline (Bronze -> Silver -> Gold)")
        run_complete_pipeline()
    elif mode == "gold-only":
        print("🔄 Running Gold Layer Only")
        try:
            normal_data, abnormal_data, summary_data = transformDataGold()
            insertDataGold(normal_data, "transactions_normal")
            insertDataGold(abnormal_data, "transactions_abnormal")
            insertDataGold(summary_data, "transactions_summary")
            print("✅ Gold layer processing completed!")
        except Exception as e:
            print(f"❌ Gold layer processing failed: {e}")
    else:
        print("❌ Invalid mode. Use: legacy, complete, or gold-only")
        print("Usage: python main.py [legacy|complete|gold-only]")

if __name__== "__main__":
    main()
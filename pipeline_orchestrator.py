#!/usr/bin/env python3
"""
Complete Data Pipeline Orchestrator
Bronze -> Silver -> Gold Processing Pipeline
"""

import pandas as pd
import time
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from databaseConfig import get_db_session, engine
from sqlalchemy import text

# Import existing modules
from getDataBronze import getDataNasabahRaw, getDataTransactionRaw
from getDataSilver import getDataCriteria
from transformDataSilver import transformDataSilver, inserDataTransaction
from transformDataGold import transformDataGold, insertDataGold

# Import monitoring system
from pipeline_config import config, monitor, PipelineStatus, LayerStatus

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.get_layer_config("monitoring").get("log_level", "INFO")),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.get_layer_config("monitoring").get("log_file", "pipeline.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """
    Orchestrates the complete data pipeline from Bronze to Silver to Gold layers
    """
    
    def __init__(self):
        self.pipeline_id = str(uuid.uuid4())[:8]
        self.start_time = None
        self.end_time = None
        self.pipeline_stats = {
            'bronze': {'status': 'pending', 'records_processed': 0, 'duration': 0},
            'silver': {'status': 'pending', 'records_processed': 0, 'duration': 0},
            'gold': {'status': 'pending', 'records_processed': 0, 'duration': 0}
        }
    
    def validate_database_connection(self) -> bool:
        """Validate database connection and schema existence"""
        try:
            db_session = get_db_session()
            if not db_session:
                logger.error("Failed to get database session")
                return False
            
            # Check if required schemas exist
            schemas_to_check = ['bronze', 'silver', 'gold']
            for schema in schemas_to_check:
                result = db_session.execute(text(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'"))
                if not result.fetchone():
                    logger.error(f"Schema '{schema}' does not exist")
                    db_session.close()
                    return False
            
            logger.info("Database connection and schemas validated successfully")
            db_session.close()
            return True
            
        except Exception as e:
            logger.error(f"Database validation failed: {e}")
            return False
    
    def process_bronze_layer(self) -> Tuple[Optional[List], Optional[List]]:
        """
        Process Bronze Layer: Extract raw data from bronze tables
        Returns: (nasabah_data, transaction_data)
        """
        logger.info("🟤 Starting Bronze Layer Processing...")
        bronze_start = time.time()
        
        # Start monitoring this layer
        layer_metrics = monitor.start_layer('bronze')
        
        try:
            # Check if bronze layer is enabled
            if not config.is_layer_enabled('bronze'):
                logger.info("Bronze layer is disabled, skipping...")
                monitor.complete_layer('bronze', 0, 0)
                return [], []
            
            # Get nasabah data
            logger.info("Extracting nasabah data from bronze layer...")
            list_nasabah_raw = getDataNasabahRaw('bronze', 'data_nasabah_raw')
            
            if list_nasabah_raw is None:
                error_msg = "Failed to extract nasabah data from bronze layer"
                logger.error(error_msg)
                monitor.complete_layer('bronze', 0, 0, error_msg)
                return None, None
            
            # Get transaction data
            logger.info("Extracting transaction data from bronze layer...")
            list_trx_raw = getDataTransactionRaw('bronze', 'transactions_raw')
            
            if list_trx_raw is None:
                error_msg = "Failed to extract transaction data from bronze layer"
                logger.error(error_msg)
                monitor.complete_layer('bronze', 0, 0, error_msg)
                return None, None
            
            bronze_duration = time.time() - bronze_start
            total_records = len(list_nasabah_raw) + len(list_trx_raw)
            
            self.pipeline_stats['bronze'] = {
                'status': 'completed',
                'records_processed': total_records,
                'duration': bronze_duration
            }
            
            # Complete layer monitoring
            monitor.complete_layer('bronze', total_records, 0)
            
            logger.info(f"✅ Bronze Layer completed: {len(list_nasabah_raw)} nasabah, {len(list_trx_raw)} transactions in {bronze_duration:.2f}s")
            return list_nasabah_raw, list_trx_raw
            
        except Exception as e:
            error_msg = f"Bronze Layer failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['bronze']['status'] = 'failed'
            monitor.complete_layer('bronze', 0, 0, error_msg)
            return None, None
    
    def process_silver_layer(self, list_nasabah: List, list_trx: List) -> Optional[pd.DataFrame]:
        """
        Process Silver Layer: Transform and validate data
        Returns: DataFrame of processed transactions
        """
        logger.info("🟡 Starting Silver Layer Processing...")
        silver_start = time.time()
        
        # Start monitoring this layer
        layer_metrics = monitor.start_layer('silver')
        
        try:
            # Check if silver layer is enabled
            if not config.is_layer_enabled('silver'):
                logger.info("Silver layer is disabled, skipping...")
                monitor.complete_layer('silver', 0, 0)
                return pd.DataFrame()
            
            # Get criteria data
            logger.info("Loading criteria data for anomaly detection...")
            criterias = getDataCriteria('silver', 'criteria')
            
            if criterias is None:
                error_msg = "Failed to load criteria data"
                logger.error(error_msg)
                monitor.complete_layer('silver', 0, 0, error_msg)
                return None
            
            # Transform data
            logger.info("Transforming bronze data to silver format...")
            list_trx_transform = transformDataSilver(
                list_trx=list_trx, 
                list_nasabah=list_nasabah, 
                list_criteria=criterias
            )
            
            if not list_trx_transform:
                logger.warning("No data to transform in silver layer")
                monitor.complete_layer('silver', 0, 0)
                return pd.DataFrame()
            
            # Insert transformed data
            logger.info("Inserting transformed data to silver layer...")
            data_inserted = inserDataTransaction(list_trx=list_trx_transform)
            
            silver_duration = time.time() - silver_start
            records_processed = len(data_inserted) if not data_inserted.empty else 0
            
            self.pipeline_stats['silver'] = {
                'status': 'completed',
                'records_processed': records_processed,
                'duration': silver_duration
            }
            
            # Complete layer monitoring
            monitor.complete_layer('silver', records_processed, 0)
            
            logger.info(f"✅ Silver Layer completed: {records_processed} records processed in {silver_duration:.2f}s")
            return data_inserted
            
        except Exception as e:
            error_msg = f"Silver Layer failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['silver']['status'] = 'failed'
            monitor.complete_layer('silver', 0, 0, error_msg)
            return None
    
    def process_gold_layer(self) -> bool:
        """
        Process Gold Layer: Create aggregated and summary data
        Returns: Success status
        """
        logger.info("🟨 Starting Gold Layer Processing...")
        gold_start = time.time()
        
        # Start monitoring this layer
        layer_metrics = monitor.start_layer('gold')
        
        try:
            # Check if gold layer is enabled
            if not config.is_layer_enabled('gold'):
                logger.info("Gold layer is disabled, skipping...")
                monitor.complete_layer('gold', 0, 0)
                return True
            
            # Transform data to gold format
            logger.info("Transforming silver data to gold format...")
            normal_data, abnormal_data, summary_data = transformDataGold()
            
            total_records = 0
            
            # Insert normal transactions
            if normal_data:
                logger.info(f"Inserting {len(normal_data)} normal transactions to gold layer...")
                insertDataGold(normal_data, "transactions_normal")
                total_records += len(normal_data)
            
            # Insert abnormal transactions
            if abnormal_data:
                logger.info(f"Inserting {len(abnormal_data)} abnormal transactions to gold layer...")
                insertDataGold(abnormal_data, "transactions_abnormal")
                total_records += len(abnormal_data)
            
            # Insert summary data
            if summary_data:
                logger.info(f"Inserting {len(summary_data)} summary records to gold layer...")
                insertDataGold(summary_data, "transactions_summary")
                total_records += len(summary_data)
            
            gold_duration = time.time() - gold_start
            self.pipeline_stats['gold'] = {
                'status': 'completed',
                'records_processed': total_records,
                'duration': gold_duration
            }
            
            # Complete layer monitoring
            monitor.complete_layer('gold', total_records, 0)
            
            logger.info(f"✅ Gold Layer completed: {total_records} records processed in {gold_duration:.2f}s")
            return True
            
        except Exception as e:
            error_msg = f"Gold Layer failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['gold']['status'] = 'failed'
            monitor.complete_layer('gold', 0, 0, error_msg)
            return False
    
    def run_complete_pipeline(self) -> bool:
        """
        Run the complete pipeline from Bronze to Gold
        Returns: Success status
        """
        self.start_time = time.time()
        logger.info(f"🚀 Starting Complete Data Pipeline: Bronze -> Silver -> Gold (ID: {self.pipeline_id})")
        
        # Start pipeline monitoring
        pipeline_metrics = monitor.start_pipeline(self.pipeline_id)
        
        try:
            # Validate database connection
            if not self.validate_database_connection():
                error_msg = "Database validation failed"
                logger.error(f"Pipeline aborted: {error_msg}")
                monitor.complete_pipeline(False, error_msg)
                return False
            
            # Step 1: Process Bronze Layer
            list_nasabah, list_trx = self.process_bronze_layer()
            if list_nasabah is None or list_trx is None:
                error_msg = "Bronze layer processing failed"
                logger.error(f"Pipeline aborted: {error_msg}")
                monitor.complete_pipeline(False, error_msg)
                return False
            
            # Step 2: Process Silver Layer
            silver_data = self.process_silver_layer(list_nasabah, list_trx)
            if silver_data is None:
                error_msg = "Silver layer processing failed"
                logger.error(f"Pipeline aborted: {error_msg}")
                monitor.complete_pipeline(False, error_msg)
                return False
            
            # Step 3: Process Gold Layer
            gold_success = self.process_gold_layer()
            if not gold_success:
                error_msg = "Gold layer processing failed"
                logger.error(f"Pipeline aborted: {error_msg}")
                monitor.complete_pipeline(False, error_msg)
                return False
            
            # Pipeline completed successfully
            self.end_time = time.time()
            total_duration = self.end_time - self.start_time
            
            logger.info("🎉 Complete Pipeline executed successfully!")
            self.print_pipeline_summary(total_duration)
            
            # Complete pipeline monitoring
            monitor.complete_pipeline(True)
            return True
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.end_time = time.time()
            monitor.complete_pipeline(False, error_msg)
            return False
    
    def print_pipeline_summary(self, total_duration: float):
        """Print comprehensive pipeline execution summary"""
        logger.info("=" * 60)
        logger.info("📊 PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        
        for layer, stats in self.pipeline_stats.items():
            status_emoji = "✅" if stats['status'] == 'completed' else "❌" if stats['status'] == 'failed' else "⏳"
            logger.info(f"{status_emoji} {layer.upper()} Layer: {stats['status']} | "
                       f"Records: {stats['records_processed']} | "
                       f"Duration: {stats['duration']:.2f}s")
        
        logger.info("-" * 60)
        logger.info(f"⏱️  Total Pipeline Duration: {total_duration:.2f} seconds")
        logger.info(f"📅 Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status and statistics"""
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'total_duration': (self.end_time - self.start_time) if self.end_time and self.start_time else None,
            'layers': self.pipeline_stats
        }


def main():
    """Main function to run the complete pipeline"""
    orchestrator = PipelineOrchestrator()
    success = orchestrator.run_complete_pipeline()
    
    if success:
        logger.info("🎯 Pipeline completed successfully!")
        exit(0)
    else:
        logger.error("💥 Pipeline failed!")
        exit(1)


if __name__ == "__main__":
    main()

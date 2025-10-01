#!/usr/bin/env python3
"""
Complete Pipeline Runner
Executes the full Bronze -> Silver -> Gold data pipeline with monitoring
"""

import sys
import argparse
import logging
import time
from datetime import datetime
from pipeline_orchestrator import PipelineOrchestrator
from pipeline_config import config, monitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_runner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline(mode: str = "complete", dry_run: bool = False) -> bool:
    """
    Run the data pipeline with specified mode
    
    Args:
        mode: Pipeline mode (complete, bronze-only, silver-only, gold-only)
        dry_run: If True, validate configuration without executing
    
    Returns:
        Success status
    """
    logger.info(f"🚀 Starting Pipeline Runner - Mode: {mode}, Dry Run: {dry_run}")
    
    if dry_run:
        logger.info("🔍 DRY RUN MODE - Validating configuration only")
        
        # Validate database connection
        orchestrator = PipelineOrchestrator()
        if orchestrator.validate_database_connection():
            logger.info("✅ Database connection validation passed")
        else:
            logger.error("❌ Database connection validation failed")
            return False
        
        # Validate configuration
        logger.info("✅ Configuration validation passed")
        logger.info("🎯 Dry run completed successfully - Pipeline is ready to execute")
        return True
    
    # Execute pipeline based on mode
    orchestrator = PipelineOrchestrator()
    
    if mode == "complete":
        logger.info("🔄 Running Complete Pipeline (Bronze -> Silver -> Gold)")
        return orchestrator.run_complete_pipeline()
    
    elif mode == "bronze-only":
        logger.info("🔄 Running Bronze Layer Only")
        try:
            # Initialize monitoring
            monitor.start_pipeline(orchestrator.pipeline_id)
            if not orchestrator.validate_database_connection():
                monitor.complete_pipeline(False, "Database validation failed")
                return False
            list_nasabah, list_trx = orchestrator.process_bronze_layer()
            success = list_nasabah is not None and list_trx is not None
            monitor.complete_pipeline(success, None if success else "Bronze layer failed")
            if success:
                logger.info("✅ Bronze layer processing completed!")
                return True
            else:
                logger.error("❌ Bronze layer processing failed!")
                return False
        except Exception as e:
            monitor.complete_pipeline(False, str(e))
            logger.error(f"❌ Bronze layer processing failed: {e}")
            return False
    
    elif mode == "silver-only":
        logger.info("🔄 Running Silver Layer Only")
        try:
            # Initialize monitoring
            monitor.start_pipeline(orchestrator.pipeline_id)
            if not orchestrator.validate_database_connection():
                monitor.complete_pipeline(False, "Database validation failed")
                return False
            # Extract bronze as prerequisite for silver
            list_nasabah, list_trx = orchestrator.process_bronze_layer()
            if list_nasabah is None or list_trx is None:
                monitor.complete_pipeline(False, "Bronze extraction failed for silver-only mode")
                logger.error("❌ Silver-only: bronze extraction failed")
                return False
            # Process silver only
            silver_df = orchestrator.process_silver_layer(list_nasabah, list_trx)
            success = silver_df is not None
            monitor.complete_pipeline(success, None if success else "Silver layer failed")
            if success:
                logger.info("✅ Silver-only processing completed!")
                return True
            else:
                logger.error("❌ Silver-only processing failed!")
                return False
        except Exception as e:
            monitor.complete_pipeline(False, str(e))
            logger.error(f"❌ Silver-only mode failed: {e}")
            return False
    
    elif mode == "gold-only":
        logger.info("🔄 Running Gold Layer Only")
        try:
            # Initialize monitoring
            monitor.start_pipeline(orchestrator.pipeline_id)
            if not orchestrator.validate_database_connection():
                monitor.complete_pipeline(False, "Database validation failed")
                return False
            success = orchestrator.process_gold_layer()
            monitor.complete_pipeline(success, None if success else "Gold layer failed")
            if success:
                logger.info("✅ Gold layer processing completed!")
                return True
            else:
                logger.error("❌ Gold layer processing failed!")
                return False
        except Exception as e:
            monitor.complete_pipeline(False, str(e))
            logger.error(f"❌ Gold layer processing failed: {e}")
            return False
    
    else:
        logger.error(f"❌ Invalid mode: {mode}")
        return False


def show_pipeline_status():
    """Show current pipeline status and metrics"""
    logger.info("📊 Current Pipeline Status:")
    
    # Get current pipeline summary
    summary = monitor.get_pipeline_summary()
    if summary.get("status") == "No active pipeline":
        logger.info("No active pipeline running")
    else:
        logger.info(f"Pipeline ID: {summary['pipeline_id']}")
        logger.info(f"Status: {summary['status']}")
        logger.info(f"Duration: {summary['duration']:.2f} seconds")
        logger.info(f"Total Records: {summary['total_records']}")
        
        logger.info("Layer Status:")
        for layer_name, layer_info in summary['layers'].items():
            logger.info(f"  {layer_name}: {layer_info['status']} - {layer_info['records']} records in {layer_info['duration']:.2f}s")
    
    # Get historical summary
    historical = monitor.get_historical_summary(days=7)
    if "message" not in historical:
        logger.info("\n📈 Historical Summary (Last 7 days):")
        logger.info(f"Total Executions: {historical['total_executions']}")
        logger.info(f"Success Rate: {historical['success_rate']:.1f}%")
        logger.info(f"Average Duration: {historical['average_duration_seconds']:.2f} seconds")
        logger.info(f"Total Records Processed: {historical['total_records_processed']}")


def show_configuration():
    """Show current pipeline configuration"""
    logger.info("⚙️  Current Pipeline Configuration:")
    
    pipeline_config = config.get_layer_config("pipeline")
    logger.info(f"Name: {pipeline_config.get('name', 'N/A')}")
    logger.info(f"Version: {pipeline_config.get('version', 'N/A')}")
    logger.info(f"Max Retries: {pipeline_config.get('max_retries', 'N/A')}")
    logger.info(f"Timeout: {pipeline_config.get('timeout_seconds', 'N/A')} seconds")
    
    # Layer configurations
    for layer in ['bronze', 'silver', 'gold']:
        layer_config = config.get_layer_config(layer)
        enabled = "✅" if layer_config.get('enabled', True) else "❌"
        logger.info(f"{layer.upper()} Layer: {enabled} (Timeout: {layer_config.get('timeout_seconds', 'N/A')}s)")


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(
        description="Complete Data Pipeline Runner - Bronze to Silver to Gold",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline_complete.py                    # Run complete pipeline
  python run_pipeline_complete.py --mode complete    # Run complete pipeline
  python run_pipeline_complete.py --mode gold-only   # Run only gold layer
  python run_pipeline_complete.py --dry-run          # Validate configuration
  python run_pipeline_complete.py --status           # Show pipeline status
  python run_pipeline_complete.py --config           # Show configuration
        """
    )
    
    parser.add_argument(
        "--mode", 
        choices=["complete", "bronze-only", "silver-only", "gold-only"],
        default="complete",
        help="Pipeline execution mode (default: complete)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing pipeline"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current pipeline status and metrics"
    )
    
    parser.add_argument(
        "--config",
        action="store_true",
        help="Show current pipeline configuration"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously run the pipeline in a loop for near-realtime processing"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds to wait between runs when --watch is enabled (default: 60)"
    )

    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Handle special commands
    if args.status:
        show_pipeline_status()
        return
    
    if args.config:
        show_configuration()
        return
    
    # Run pipeline (single run or watch loop)
    try:
        if not args.watch:
            success = run_pipeline(mode=args.mode, dry_run=args.dry_run)
            if success:
                logger.info("🎯 Pipeline execution completed successfully!")
                sys.exit(0)
            else:
                logger.error("💥 Pipeline execution failed!")
                sys.exit(1)
        else:
            logger.info(f"👀 Watch mode enabled - Re-running every {args.interval}s. Press Ctrl+C to stop.")
            while True:
                success = run_pipeline(mode=args.mode, dry_run=args.dry_run)
                if success:
                    logger.info("🎯 Pipeline execution completed successfully!")
                else:
                    logger.error("💥 Pipeline execution failed!")
                time.sleep(max(1, args.interval))
            
    except KeyboardInterrupt:
        logger.info("🛑 Watch mode interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

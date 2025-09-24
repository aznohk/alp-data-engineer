#!/usr/bin/env python3
"""
Pipeline Configuration and Monitoring System
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum


class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LayerStatus(Enum):
    """Individual layer status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class LayerMetrics:
    """Metrics for individual pipeline layer"""
    layer_name: str
    status: LayerStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    def calculate_duration(self):
        """Calculate duration if both start and end times are set"""
        if self.start_time and self.end_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        # Convert datetime objects to strings
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        return data


@dataclass
class PipelineMetrics:
    """Complete pipeline execution metrics"""
    pipeline_id: str
    status: PipelineStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_duration_seconds: float = 0.0
    layers: Dict[str, LayerMetrics] = None
    total_records_processed: int = 0
    total_records_failed: int = 0
    error_message: Optional[str] = None
    configuration: Dict[str, Any] = None

    def __post_init__(self):
        if self.layers is None:
            self.layers = {}
        if self.configuration is None:
            self.configuration = {}

    def calculate_total_duration(self):
        """Calculate total pipeline duration"""
        if self.start_time and self.end_time:
            self.total_duration_seconds = (self.end_time - self.start_time).total_seconds()

    def calculate_totals(self):
        """Calculate total records processed and failed"""
        self.total_records_processed = sum(layer.records_processed for layer in self.layers.values())
        self.total_records_failed = sum(layer.records_failed for layer in self.layers.values())

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        # Convert datetime objects to strings
        if self.start_time:
            data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        # Convert layer metrics
        data['layers'] = {name: layer.to_dict() for name, layer in self.layers.items()}
        return data


class PipelineConfig:
    """Pipeline configuration management"""
    
    def __init__(self, config_file: str = "pipeline_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self) -> Dict:
        """Load configuration from file or create default"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Failed to load config file {self.config_file}: {e}")
        
        # Return default configuration
        return self.get_default_config()
    
    def get_default_config(self) -> Dict:
        """Get default pipeline configuration"""
        return {
            "pipeline": {
                "name": "Bronze-Silver-Gold Data Pipeline",
                "version": "1.0.0",
                "description": "Complete data processing pipeline from bronze to gold layers",
                "max_retries": 3,
                "retry_delay_seconds": 30,
                "timeout_seconds": 3600
            },
            "bronze": {
                "enabled": True,
                "batch_size": 1000,
                "timeout_seconds": 300,
                "tables": {
                    "data_nasabah_raw": {"required": True},
                    "transactions_raw": {"required": True}
                }
            },
            "silver": {
                "enabled": True,
                "batch_size": 1000,
                "timeout_seconds": 600,
                "tables": {
                    "criteria": {"required": True},
                    "transactions": {"required": True}
                },
                "anomaly_detection": {
                    "enabled": True,
                    "time_threshold_seconds": 3600,
                    "amount_threshold": 200000
                }
            },
            "gold": {
                "enabled": True,
                "batch_size": 1000,
                "timeout_seconds": 300,
                "tables": {
                    "transactions_normal": {"required": True},
                    "transactions_abnormal": {"required": True},
                    "transactions_summary": {"required": True}
                }
            },
            "monitoring": {
                "log_level": "INFO",
                "log_file": "pipeline.log",
                "metrics_file": "pipeline_metrics.json",
                "enable_notifications": False,
                "notification_email": None
            },
            "database": {
                "connection_pool_size": 10,
                "query_timeout_seconds": 60,
                "enable_connection_validation": True
            }
        }
    
    def save_config(self):
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logging.info(f"Configuration saved to {self.config_file}")
        except Exception as e:
            logging.error(f"Failed to save configuration: {e}")
    
    def get_layer_config(self, layer_name: str) -> Dict:
        """Get configuration for specific layer"""
        return self.config.get(layer_name, {})
    
    def is_layer_enabled(self, layer_name: str) -> bool:
        """Check if layer is enabled"""
        layer_config = self.get_layer_config(layer_name)
        return layer_config.get("enabled", True)
    
    def get_timeout(self, layer_name: str) -> int:
        """Get timeout for specific layer"""
        layer_config = self.get_layer_config(layer_name)
        return layer_config.get("timeout_seconds", 300)


class PipelineMonitor:
    """Pipeline monitoring and metrics collection"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.current_pipeline: Optional[PipelineMetrics] = None
        self.metrics_history: List[PipelineMetrics] = []
        self.logger = logging.getLogger(__name__)
    
    def start_pipeline(self, pipeline_id: str) -> PipelineMetrics:
        """Start monitoring a new pipeline execution"""
        self.current_pipeline = PipelineMetrics(
            pipeline_id=pipeline_id,
            status=PipelineStatus.RUNNING,
            start_time=datetime.now(),
            configuration=self.config.config.copy()
        )
        
        self.logger.info(f"🚀 Started monitoring pipeline: {pipeline_id}")
        return self.current_pipeline
    
    def start_layer(self, layer_name: str) -> LayerMetrics:
        """Start monitoring a layer execution"""
        if not self.current_pipeline:
            raise RuntimeError("No active pipeline to monitor")
        
        layer_metrics = LayerMetrics(
            layer_name=layer_name,
            status=LayerStatus.RUNNING,
            start_time=datetime.now()
        )
        
        self.current_pipeline.layers[layer_name] = layer_metrics
        self.logger.info(f"🔄 Started layer: {layer_name}")
        return layer_metrics
    
    def complete_layer(self, layer_name: str, records_processed: int = 0, 
                      records_failed: int = 0, error_message: str = None):
        """Mark layer as completed"""
        if not self.current_pipeline or layer_name not in self.current_pipeline.layers:
            self.logger.warning(f"Layer {layer_name} not found in current pipeline")
            return
        
        layer = self.current_pipeline.layers[layer_name]
        layer.end_time = datetime.now()
        layer.records_processed = records_processed
        layer.records_failed = records_failed
        layer.calculate_duration()
        
        if error_message:
            layer.status = LayerStatus.FAILED
            layer.error_message = error_message
            self.logger.error(f"❌ Layer {layer_name} failed: {error_message}")
        else:
            layer.status = LayerStatus.COMPLETED
            self.logger.info(f"✅ Layer {layer_name} completed: {records_processed} records in {layer.duration_seconds:.2f}s")
    
    def complete_pipeline(self, success: bool = True, error_message: str = None):
        """Mark pipeline as completed"""
        if not self.current_pipeline:
            self.logger.warning("No active pipeline to complete")
            return
        
        self.current_pipeline.end_time = datetime.now()
        self.current_pipeline.calculate_total_duration()
        self.current_pipeline.calculate_totals()
        
        if success:
            self.current_pipeline.status = PipelineStatus.COMPLETED
            self.logger.info(f"🎉 Pipeline {self.current_pipeline.pipeline_id} completed successfully!")
        else:
            self.current_pipeline.status = PipelineStatus.FAILED
            self.current_pipeline.error_message = error_message
            self.logger.error(f"💥 Pipeline {self.current_pipeline.pipeline_id} failed: {error_message}")
        
        # Save metrics
        self.save_metrics()
        
        # Add to history
        self.metrics_history.append(self.current_pipeline)
        
        # Keep only last 100 executions
        if len(self.metrics_history) > 100:
            self.metrics_history = self.metrics_history[-100:]
        
        self.current_pipeline = None
    
    def save_metrics(self):
        """Save current metrics to file"""
        if not self.current_pipeline:
            return
        
        metrics_file = self.config.get_layer_config("monitoring").get("metrics_file", "pipeline_metrics.json")
        
        try:
            # Load existing metrics
            all_metrics = []
            if os.path.exists(metrics_file):
                with open(metrics_file, 'r') as f:
                    all_metrics = json.load(f)
            
            # Add current metrics
            all_metrics.append(self.current_pipeline.to_dict())
            
            # Keep only last 50 executions
            if len(all_metrics) > 50:
                all_metrics = all_metrics[-50:]
            
            # Save updated metrics
            with open(metrics_file, 'w') as f:
                json.dump(all_metrics, f, indent=2)
            
            self.logger.info(f"Metrics saved to {metrics_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save metrics: {e}")
    
    def get_pipeline_summary(self) -> Dict:
        """Get summary of current pipeline execution"""
        if not self.current_pipeline:
            return {"status": "No active pipeline"}
        
        return {
            "pipeline_id": self.current_pipeline.pipeline_id,
            "status": self.current_pipeline.status.value,
            "duration": self.current_pipeline.total_duration_seconds,
            "total_records": self.current_pipeline.total_records_processed,
            "layers": {
                name: {
                    "status": layer.status.value,
                    "duration": layer.duration_seconds,
                    "records": layer.records_processed
                }
                for name, layer in self.current_pipeline.layers.items()
            }
        }
    
    def get_historical_summary(self, days: int = 7) -> Dict:
        """Get summary of pipeline executions in the last N days"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_pipelines = [
            p for p in self.metrics_history 
            if p.start_time and p.start_time >= cutoff_date
        ]
        
        if not recent_pipelines:
            return {"message": f"No pipeline executions found in the last {days} days"}
        
        total_executions = len(recent_pipelines)
        successful_executions = len([p for p in recent_pipelines if p.status == PipelineStatus.COMPLETED])
        failed_executions = total_executions - successful_executions
        
        avg_duration = sum(p.total_duration_seconds for p in recent_pipelines) / total_executions
        total_records = sum(p.total_records_processed for p in recent_pipelines)
        
        return {
            "period_days": days,
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": (successful_executions / total_executions) * 100,
            "average_duration_seconds": avg_duration,
            "total_records_processed": total_records
        }


# Global instances
config = PipelineConfig()
monitor = PipelineMonitor(config)

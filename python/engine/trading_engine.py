import asyncio
import logging
import time
import yaml
import os
from typing import Dict, List, Optional, Any, Set, Callable
from datetime import datetime
import signal
import json

from .data_structures import Event, EventType, Order, Trade, Position, MarketData
from .order_manager import OrderManager
from .position_manager import PositionManager
from .risk_manager import RiskManager
from .event_processor import EventProcessor


logger = logging.getLogger(__name__)


class TradingEngine:
    def __init__(self, config_path: str):
        """Initialize the trading engine with the specified configuration"""
        self.config = self._load_config(config_path)
        self.name = self.config.get("engine_name", "TradingEngine")
        self.instance_id = self.config.get("instance_id", "main")
        
        # Set up logging
        self._configure_logging()
        
        # Create event processor
        max_queue_size = self.config.get("event_queue_size", 100000)
        self.event_processor = EventProcessor(max_queue_size=max_queue_size)
        
        # Create core components
        self.order_manager = OrderManager(self.event_processor)
        self.position_manager = PositionManager(self.event_processor)
        
        # Initialize risk manager with limits from config
        risk_limits = self.config.get("risk_limits", {})
        self.risk_manager = RiskManager(
            self.event_processor, 
            self.order_manager, 
            self.position_manager,
            risk_limits
        )
        
        # Engine state
        self.running = False
        self.startup_time = None
        self.shutdown_time = None
        self.last_heartbeat = None
        self.registered_data_sources = set()
        self.registered_strategies = {}
        self.stats = {
            "events_processed": 0,
            "orders_submitted": 0,
            "trades_executed": 0
        }
        
        # Additional components from config
        self.initialize_additional_components()
        
        # Register for events
        self.event_processor.add_handler(EventType.SYSTEM_EVENT, self._handle_system_event)
        self.event_processor.add_handler(EventType.ORDER_UPDATE, self._handle_order_update)
        self.event_processor.add_handler(EventType.TRADE_UPDATE, self._handle_trade_update)
        
        logger.info(f"Trading engine {self.name} initialized with instance ID {self.instance_id}")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_path}: {e}")
            # Return default configuration
            return {
                "engine_name": "TradingEngine",
                "instance_id": "default",
                "log_level": "INFO",
                "event_queue_size": 100000,
                "heartbeat_interval_seconds": 5,
                "risk_limits": {}
            }
    
    def _configure_logging(self):
        """Configure logging based on settings in config"""
        log_level = getattr(logging, self.config.get("log_level", "INFO"))
        log_format = self.config.get("log_format", 
                                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        # Configure root logger
        logging.basicConfig(
            level=log_level,
            format=log_format
        )
        
        # Configure file handler if specified
        log_file = self.config.get("log_file")
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
    
    def initialize_additional_components(self):
        """Initialize any additional components specified in config"""
        # This could include custom market data handlers, strategy loaders, etc.
        pass
    
    async def _handle_system_event(self, event: Event):
        """Process system events"""
        if not isinstance(event.data, dict):
            return
        
        event_subtype = event.data.get("type")
        
        if event_subtype == "shutdown":
            logger.info("Received shutdown event, initiating shutdown...")
            await self.stop()
        elif event_subtype == "heartbeat_request":
            # Respond with heartbeat
            await self.send_heartbeat()
        elif event_subtype == "status_request":
            # Respond with status
            await self.publish_status()
    
    async def _handle_order_update(self, event: Event):
        """Track order update events for statistics"""
        order = event.data
        if order.status.name == "PENDING_NEW":
            self.stats["orders_submitted"] += 1
    
    async def _handle_trade_update(self, event: Event):
        """Track trade events for statistics"""
        self.stats["trades_executed"] += 1
    
    async def start(self):
        """Start the trading engine and all components"""
        if self.running:
            logger.warning("Trading engine already running")
            return
        
        logger.info(f"Starting trading engine {self.name}...")
        self.running = True
        self.startup_time = datetime.utcnow()
        
        # Start event processor
        await self.event_processor.start()
        
        # Start risk manager periodic checks
        await self.risk_manager.start_periodic_checks()
        
        # Set up heartbeat task
        heartbeat_interval = self.config.get("heartbeat_interval_seconds", 5)
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop(heartbeat_interval))
        
        # Publish startup event
        await self.event_processor.publish(Event(
            event_type=EventType.SYSTEM_EVENT,
            data={
                "type": "startup",
                "engine_name": self.name,
                "instance_id": self.instance_id,
                "timestamp": self.startup_time.isoformat(),
                "config": {k: v for k, v in self.config.items() if k != "api_keys"}  # Don't log sensitive data
            },
            source="trading_engine"
        ))
        
        logger.info(f"Trading engine {self.name} started successfully")
    
    async def stop(self):
        """Stop the trading engine and all components"""
        if not self.running:
            logger.warning("Trading engine already stopped")
            return
        
        logger.info(f"Stopping trading engine {self.name}...")
        self.running = False
        self.shutdown_time = datetime.utcnow()
        
        # Cancel heartbeat task
        if hasattr(self, 'heartbeat_task'):
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Stop risk manager
        await self.risk_manager.stop_periodic_checks()
        
        # Publish shutdown event
        await self.event_processor.publish(Event(
            event_type=EventType.SYSTEM_EVENT,
            data={
                "type": "shutdown",
                "engine_name": self.name,
                "instance_id": self.instance_id,
                "timestamp": self.shutdown_time.isoformat(),
                "uptime_seconds": (self.shutdown_time - self.startup_time).total_seconds(),
                "stats": self.stats
            },
            source="trading_engine"
        ))
        
        # Stop event processor (this should be last)
        await self.event_processor.stop()
        
        logger.info(f"Trading engine {self.name} stopped successfully")
    
    async def _heartbeat_loop(self, interval_seconds: int):
        """Send periodic heartbeats to indicate the engine is running"""
        try:
            while self.running:
                await self.send_heartbeat()
                await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            logger.debug("Heartbeat task cancelled")
            raise
    
    async def send_heartbeat(self):
        """Send a heartbeat event"""
        self.last_heartbeat = datetime.utcnow()
        
        # Get basic status info
        event_queue_size = self.event_processor.event_queue.qsize()
        order_stats = self.order_manager.get_order_statistics()
        position_stats = self.position_manager.get_position_statistics()
        
        await self.event_processor.publish(Event(
            event_type=EventType.SYSTEM_EVENT,
            data={
                "type": "heartbeat",
                "engine_name": self.name,
                "instance_id": self.instance_id,
                "timestamp": self.last_heartbeat.isoformat(),
                "uptime_seconds": (self.last_heartbeat - self.startup_time).total_seconds() if self.startup_time else 0,
                "event_queue_size": event_queue_size,
                "active_orders": order_stats["active_orders"],
                "positions_count": position_stats["position_count"]
            },
            source="trading_engine",
            priority=3  # Lower priority for heartbeats
        ))
    
    async def publish_status(self):
        """Publish a comprehensive status update"""
        now = datetime.utcnow()
        
        # Gather performance metrics
        event_metrics = self.event_processor.get_performance_metrics()
        order_stats = self.order_manager.get_order_statistics()
        position_stats = self.position_manager.get_position_statistics()
        risk_summary = self.risk_manager.get_risk_summary()
        
        # Publish status event
        await self.event_processor.publish(Event(
            event_type=EventType.SYSTEM_EVENT,
            data={
                "type": "status",
                "engine_name": self.name,
                "instance_id": self.instance_id,
                "timestamp": now.isoformat(),
                "uptime_seconds": (now - self.startup_time).total_seconds() if self.startup_time else 0,
                "performance": event_metrics,
                "orders": order_stats,
                "positions": position_stats,
                "risk": risk_summary,
                "stats": self.stats
            },
            source="trading_engine"
        ))
    
    async def register_data_source(self, source_id: str):
        """Register a market data source with the engine"""
        self.registered_data_sources.add(source_id)
        logger.info(f"Registered data source: {source_id}")
        
        # Publish registration event
        await self.event_processor.publish(Event(
            event_type=EventType.SYSTEM_EVENT,
            data={
                "type": "data_source_registered",
                "source_id": source_id,
                "timestamp": datetime.utcnow().isoformat()
            },
            source="trading_engine"
        ))
    
    async def register_strategy(self, strategy_id: str, strategy_info: Dict):
        """Register a trading strategy with the engine"""
        self.registered_strategies[strategy_id] = {
            "info": strategy_info,
            "registered_at": datetime.utcnow()
        }
        logger.info(f"Registered strategy: {strategy_id}")
        
        # Publish registration event
        await self.event_processor.publish(Event(
            event_type=EventType.SYSTEM_EVENT,
            data={
                "type": "strategy_registered",
                "strategy_id": strategy_id,
                "strategy_info": strategy_info,
                "timestamp": datetime.utcnow().isoformat()
            },
            source="trading_engine"
        ))
    
    async def process_market_data(self, market_data: MarketData):
        """Process incoming market data"""
        # Create and publish a market data event
        await self.event_processor.publish(Event(
            event_type=EventType.MARKET_DATA,
            data=market_data,
            source=market_data.source,
            sequence_id=market_data.sequence_id
        ))
    
    async def submit_order(self, order: Order, 
                          callback: Optional[Callable[[Order], Any]] = None) -> str:
        """Submit an order to the trading system"""
        # Submit to order manager
        order_id = await self.order_manager.submit_order(order, callback)
        return order_id
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        return await self.order_manager.cancel_order(order_id)
    
    def get_position(self, instrument_id: str) -> Position:
        """Get the current position for an instrument"""
        return self.position_manager.get_position(instrument_id)
    
    def get_all_positions(self) -> List[Position]:
        """Get all current positions"""
        return self.position_manager.get_all_positions()
    
    def get_engine_status(self) -> Dict:
        """Get a comprehensive status of the trading engine"""
        now = datetime.utcnow()
        
        # Basic status
        status = {
            "engine_name": self.name,
            "instance_id": self.instance_id,
            "running": self.running,
            "startup_time": self.startup_time.isoformat() if self.startup_time else None,
            "current_time": now.isoformat(),
            "uptime_seconds": (now - self.startup_time).total_seconds() if self.startup_time else 0,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
        }
        
        # Component statuses
        status["order_manager"] = self.order_manager.get_order_statistics()
        status["position_manager"] = self.position_manager.get_position_statistics()
        status["risk_manager"] = {
            "rules": self.risk_manager.get_rule_status(),
            "summary": self.risk_manager.get_risk_summary()
        }
        status["event_processor"] = self.event_processor.get_performance_metrics()
        
        # Additional stats
        status["stats"] = self.stats
        status["data_sources"] = list(self.registered_data_sources)
        status["strategies"] = {
            sid: {
                "registered_at": info["registered_at"].isoformat(),
                **{k: v for k, v in info["info"].items() if k != "parameters"}  # Don't include all parameters
            }
            for sid, info in self.registered_strategies.items()
        }
        
        return status


# Utility to handle signals for graceful shutdown
def setup_signal_handlers(trading_engine, loop):
    """Set up signal handlers for graceful shutdown"""
    
    def signal_handler():
        logger.info("Received shutdown signal, initiating graceful shutdown...")
        asyncio.create_task(trading_engine.stop())
    
    for signal_name in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signal_name),
            signal_handler
        )
import asyncio
import logging
from typing import Dict, List, Optional, Set, Callable, Awaitable, Tuple, Any
from datetime import datetime, timedelta
import json
import numpy as np

from .data_structures import Order, Position, OrderSide, OrderType, Event, EventType
from .order_manager import OrderManager
from .data_structures import OrderStatus

logger = logging.getLogger(__name__)


class RiskRule:
    """Base class for risk rules"""
    def __init__(self, name: str, enabled: bool = True):
        self.name = name
        self.enabled = enabled
        self.violations = 0
        self.last_check_time = datetime.utcnow()
    
    async def check(self, risk_manager: 'RiskManager', context: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check if the rule is violated
        Returns (passed, message)
        """
        self.last_check_time = datetime.utcnow()
        return True, "Rule passed"


class PositionLimitRule(RiskRule):
    """Rule to enforce maximum position size"""
    def __init__(self, instrument_id: str, max_position: float, name: Optional[str] = None, enabled: bool = True):
        super().__init__(name or f"Position limit for {instrument_id}", enabled)
        self.instrument_id = instrument_id
        self.max_position = max_position
    
    async def check(self, risk_manager: 'RiskManager', context: Dict[str, Any]) -> Tuple[bool, str]:
        await super().check(risk_manager, context)
        
        # Check if this applies to the current order
        order = context.get('order')
        if order and order.instrument_id != self.instrument_id:
            return True, "Rule not applicable to this instrument"
        
        # Get current position
        position = risk_manager.position_manager.get_position(self.instrument_id)
        current_position = abs(position.quantity)
        
        # For new orders, check if the order would exceed the limit
        if order:
            new_position = current_position
            if order.side == OrderSide.BUY:
                new_position = abs(position.quantity + order.quantity)
            else:
                new_position = abs(position.quantity - order.quantity)
            
            if new_position > self.max_position:
                self.violations += 1
                return False, f"Order would exceed position limit of {self.max_position} for {self.instrument_id}"
        
        # For position checks, just check current position
        elif current_position > self.max_position:
            self.violations += 1
            return False, f"Current position of {current_position} exceeds limit of {self.max_position} for {self.instrument_id}"
        
        return True, "Position within limits"


class DrawdownLimitRule(RiskRule):
    """Rule to enforce maximum drawdown"""
    def __init__(self, max_drawdown_pct: float, window_days: int = 1, name: Optional[str] = None, enabled: bool = True):
        super().__init__(name or f"Drawdown limit of {max_drawdown_pct}%", enabled)
        self.max_drawdown_pct = max_drawdown_pct
        self.window_days = window_days
        self.peak_value = None
    
    async def check(self, risk_manager: 'RiskManager', context: Dict[str, Any]) -> Tuple[bool, str]:
        await super().check(risk_manager, context)
        
        # Get current portfolio value
        pnl_summary = risk_manager.position_manager.get_pnl_summary()
        current_value = pnl_summary["realized_pnl"] + pnl_summary["unrealized_pnl"]
        
        # Initialize peak if not set
        if self.peak_value is None:
            self.peak_value = current_value
        elif current_value > self.peak_value:
            self.peak_value = current_value
        
        # Calculate drawdown
        if self.peak_value <= 0:
            return True, "No peak value established yet"
        
        drawdown_pct = (self.peak_value - current_value) / abs(self.peak_value) * 100
        
        if drawdown_pct > self.max_drawdown_pct:
            self.violations += 1
            return False, f"Current drawdown of {drawdown_pct:.2f}% exceeds limit of {self.max_drawdown_pct}%"
        
        return True, f"Current drawdown of {drawdown_pct:.2f}% within limits"


class ExposureByStrategyRule(RiskRule):
    """Rule to limit exposure by strategy"""
    def __init__(self, strategy_id: str, max_exposure: float, name: Optional[str] = None, enabled: bool = True):
        super().__init__(name or f"Exposure limit for strategy {strategy_id}", enabled)
        self.strategy_id = strategy_id
        self.max_exposure = max_exposure
    
    async def check(self, risk_manager: 'RiskManager', context: Dict[str, Any]) -> Tuple[bool, str]:
        await super().check(risk_manager, context)
        
        # Check if this applies to the current order
        order = context.get('order')
        if order and order.strategy_id != self.strategy_id:
            return True, "Rule not applicable to this strategy"
        
        # Calculate strategy exposure
        strategy_positions = risk_manager.position_manager.get_strategy_exposure(self.strategy_id)
        total_exposure = 0.0
        
        for instrument_id, quantity in strategy_positions.items():
            position = risk_manager.position_manager.get_position(instrument_id)
            if position.current_price:
                exposure = abs(quantity * position.current_price)
                total_exposure += exposure
        
        # For new orders, add potential exposure
        if order and order.strategy_id == self.strategy_id:
            # Get price from order or position
            price = order.price
            if not price:
                position = risk_manager.position_manager.get_position(order.instrument_id)
                price = position.current_price or 0
            
            additional_exposure = order.quantity * price
            new_exposure = total_exposure + additional_exposure
            
            if new_exposure > self.max_exposure:
                self.violations += 1
                return False, f"Order would exceed exposure limit of {self.max_exposure} for strategy {self.strategy_id}"
        
        # For position checks, just check current exposure
        elif total_exposure > self.max_exposure:
            self.violations += 1
            return False, f"Current exposure of {total_exposure} exceeds limit of {self.max_exposure} for strategy {self.strategy_id}"
        
        return True, f"Strategy exposure of {total_exposure} within limits"


class RiskManager:
    def __init__(self, event_processor, order_manager, position_manager, config=None):
        self.event_processor = event_processor
        self.order_manager = order_manager
        self.position_manager = position_manager
        
        # Risk rules
        self.rules: Dict[str, RiskRule] = {}
        
        # Risk limits from config
        self.config = config or {}
        self.initialize_rules_from_config()
        
        # Periodic check task
        self.periodic_check_task = None
        self.check_interval_seconds = 60  # Default to checking every minute
        
        # Register for events
        self.event_processor.add_handler(EventType.ORDER_UPDATE, self._handle_order_update)
        self.event_processor.add_handler(EventType.POSITION_UPDATE, self._handle_position_update)
    
    def initialize_rules_from_config(self):
        """Initialize risk rules from configuration"""
        if not self.config:
            return
        
        # Position limits
        if "position_limits" in self.config:
            for instrument, limit in self.config["position_limits"].items():
                rule = PositionLimitRule(instrument, float(limit))
                self.add_rule(rule)
        
        # Drawdown limit
        if "max_drawdown_pct" in self.config:
            drawdown_window = self.config.get("drawdown_window_days", 1)
            rule = DrawdownLimitRule(float(self.config["max_drawdown_pct"]), drawdown_window)
            self.add_rule(rule)
        
        # Strategy exposure limits
        if "strategy_exposure_limits" in self.config:
            for strategy, limit in self.config["strategy_exposure_limits"].items():
                rule = ExposureByStrategyRule(strategy, float(limit))
                self.add_rule(rule)
    
    def add_rule(self, rule: RiskRule):
        """Add a risk rule"""
        self.rules[rule.name] = rule
    
    def remove_rule(self, rule_name: str) -> bool:
        """Remove a risk rule"""
        if rule_name in self.rules:
            del self.rules[rule_name]
            return True
        return False
    
    async def _handle_order_update(self, event: Event):
        """Process order update events"""
        order = event.data
        
        # Only check new orders
        if order.status != OrderStatus.PENDING_NEW:
            return
        
        # Check risk rules for this order
        context = {"order": order, "event_type": "order"}
        passed, messages = await self.check_rules(context)
        
        if not passed:
            # Reject the order
            order.status = OrderStatus.REJECTED
            order.updated_at = datetime.utcnow()
            
            logger.warning(f"Order {order.order_id} rejected due to risk check failure: {', '.join(messages)}")
            
            # Publish order update with rejection
            await self.event_processor.publish(Event(
                event_type=EventType.ORDER_UPDATE,
                data=order,
                source="risk_manager"
            ))
            
            # Also publish risk event
            await self.event_processor.publish(Event(
                event_type=EventType.RISK_CHECK,
                data={
                    "passed": False,
                    "order_id": order.order_id,
                    "messages": messages,
                    "timestamp": datetime.utcnow()
                },
                source="risk_manager"
            ))
    
    async def _handle_position_update(self, event: Event):
        """Process position update events"""
        position = event.data
        
        # Periodic check will handle most position-based rules
        # This handler could implement more immediate checks if needed
        pass
    
    async def check_rules(self, context: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Check all enabled risk rules
        Returns (all_passed, error_messages)
        """
        all_passed = True
        messages = []
        
        for rule_name, rule in self.rules.items():
            if not rule.enabled:
                continue
                
            try:
                passed, message = await rule.check(self, context)
                if not passed:
                    all_passed = False
                    messages.append(f"{rule_name}: {message}")
            except Exception as e:
                logger.exception(f"Error checking risk rule {rule_name}: {e}")
                all_passed = False
                messages.append(f"{rule_name}: Error during check - {str(e)}")
        
        return all_passed, messages
    
    async def start_periodic_checks(self):
        """Start periodic risk checks"""
        self.periodic_check_task = asyncio.create_task(self._periodic_check_loop())
    
    async def stop_periodic_checks(self):
        """Stop periodic risk checks"""
        if self.periodic_check_task:
            self.periodic_check_task.cancel()
            try:
                await self.periodic_check_task
            except asyncio.CancelledError:
                pass
            self.periodic_check_task = None
    
    async def _periodic_check_loop(self):
        """Background task to periodically check risk rules"""
        try:
            while True:
                await self._perform_periodic_check()
                await asyncio.sleep(self.check_interval_seconds)
        except asyncio.CancelledError:
            logger.info("Periodic risk check task cancelled")
            raise
        except Exception as e:
            logger.exception(f"Error in periodic risk check: {e}")
    
    async def _perform_periodic_check(self):
        """Perform a comprehensive risk check"""
        context = {"event_type": "periodic"}
        passed, messages = await self.check_rules(context)
        
        if not passed:
            logger.warning(f"Periodic risk check failed: {', '.join(messages)}")
            
            # Publish risk event
            await self.event_processor.publish(Event(
                event_type=EventType.RISK_CHECK,
                data={
                    "passed": False,
                    "messages": messages,
                    "timestamp": datetime.utcnow(),
                    "check_type": "periodic"
                },
                source="risk_manager"
            ))
            
            # Here we could implement automatic remediation actions:
            # - Cancel open orders
            # - Reduce positions
            # - Notify administrators
    
    def get_rule_status(self) -> List[Dict]:
        """Get the status of all risk rules"""
        result = []
        for name, rule in self.rules.items():
            result.append({
                "name": name,
                "enabled": rule.enabled,
                "violations": rule.violations,
                "last_check": rule.last_check_time.isoformat(),
                "type": rule.__class__.__name__
            })
        return result
    
    def get_risk_summary(self) -> Dict:
        """Get a summary of the current risk state"""
        # Get position data
        positions = self.position_manager.get_all_positions()
        
        # Calculate various risk metrics
        gross_exposure = sum(abs(p.position_value) for p in positions)
        net_exposure = sum(p.position_value for p in positions)
        long_exposure = sum(p.position_value for p in positions if p.quantity > 0)
        short_exposure = sum(p.position_value for p in positions if p.quantity < 0)
        
        # Calculate some basic portfolio statistics
        pnl_values = [p.unrealized_pnl for p in positions]
        pnl_std = np.std(pnl_values) if pnl_values else 0
        
        return {
            "gross_exposure": gross_exposure,
            "net_exposure": net_exposure,
            "long_exposure": long_exposure,
            "short_exposure": short_exposure,
            "long_short_ratio": long_exposure / abs(short_exposure) if short_exposure else float('inf'),
            "pnl_volatility": pnl_std,
            "rule_violations": sum(rule.violations for rule in self.rules.values()),
            "active_rules": sum(1 for rule in self.rules.values() if rule.enabled),
            "timestamp": datetime.utcnow().isoformat()
        }
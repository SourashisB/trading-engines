import asyncio
import logging
import time
from typing import Dict, List, Optional, Set, Callable, Awaitable
from datetime import datetime
import json

from .data_structures import Order, Trade, OrderStatus, OrderType, Event, EventType

logger = logging.getLogger(__name__)


class OrderManager:
    def __init__(self, event_processor):
        self.orders: Dict[str, Order] = {}  # order_id -> Order
        self.active_orders: Set[str] = set()  # Set of active order_ids
        self.order_history: Dict[str, List[Order]] = {}  # order_id -> list of state changes
        self.trades: Dict[str, List[Trade]] = {}  # order_id -> list of trades
        
        self.event_processor = event_processor
        self.order_handlers: Dict[str, Callable[[Order], Awaitable[None]]] = {}
        
        # Register for order and trade update events
        self.event_processor.add_handler(EventType.ORDER_UPDATE, self._handle_order_update)
        self.event_processor.add_handler(EventType.TRADE_UPDATE, self._handle_trade_update)
    
    async def _handle_order_update(self, event: Event):
        """Process order update events"""
        order = event.data
        if not isinstance(order, Order):
            logger.error(f"Invalid order data in event: {event}")
            return
        
        # Update the order in our state
        self._update_order_state(order)
        
        # Call any registered callback for this order
        if order.order_id in self.order_handlers:
            try:
                await self.order_handlers[order.order_id](order)
            except Exception as e:
                logger.exception(f"Error in order handler for {order.order_id}: {e}")
    
    async def _handle_trade_update(self, event: Event):
        """Process trade update events"""
        trade = event.data
        if not isinstance(trade, Trade):
            logger.error(f"Invalid trade data in event: {event}")
            return
        
        # Store the trade
        if trade.order_id not in self.trades:
            self.trades[trade.order_id] = []
        self.trades[trade.order_id].append(trade)
        
        # Update the corresponding order
        if trade.order_id in self.orders:
            order = self.orders[trade.order_id]
            order.filled_quantity += trade.quantity
            
            # Calculate average fill price
            if order.average_fill_price is None:
                order.average_fill_price = trade.price
            else:
                prev_fill_qty = order.filled_quantity - trade.quantity
                order.average_fill_price = (
                    (order.average_fill_price * prev_fill_qty + trade.price * trade.quantity) / 
                    order.filled_quantity
                )
            
            # Update order status
            if abs(order.filled_quantity - order.quantity) < 1e-10:  # Filled completely
                order.status = OrderStatus.FILLED
                self.active_orders.discard(order.order_id)
            elif order.filled_quantity > 0:  # Partially filled
                order.status = OrderStatus.PARTIALLY_FILLED
            
            order.updated_at = datetime.utcnow()
            
            # Save order state change
            self._save_order_history(order)
            
            # Publish order update event
            await self.event_processor.publish(Event(
                event_type=EventType.ORDER_UPDATE,
                data=order,
                source="order_manager"
            ))
    
    def _update_order_state(self, order: Order):
        """Update the internal order state"""
        # Store the order
        self.orders[order.order_id] = order
        
        # Update active orders set
        if order.is_active():
            self.active_orders.add(order.order_id)
        else:
            self.active_orders.discard(order.order_id)
        
        # Save order state change
        self._save_order_history(order)
    
    def _save_order_history(self, order: Order):
        """Save a snapshot of the order state in history"""
        if order.order_id not in self.order_history:
            self.order_history[order.order_id] = []
        
        # Create a copy of the order for history
        order_copy = Order.from_dict(order.to_dict())
        self.order_history[order.order_id].append(order_copy)
    
    async def submit_order(self, order: Order, 
                          callback: Optional[Callable[[Order], Awaitable[None]]] = None) -> str:
        """
        Submit an order to the trading system
        Returns the order ID
        """
        # Set initial order state
        order.status = OrderStatus.PENDING_NEW
        order.created_at = datetime.utcnow()
        order.updated_at = order.created_at
        
        # Store the order
        self._update_order_state(order)
        
        # Register callback if provided
        if callback:
            self.order_handlers[order.order_id] = callback
        
        # Publish order creation event
        await self.event_processor.publish(Event(
            event_type=EventType.ORDER_UPDATE,
            data=order,
            source="order_manager"
        ))
        
        return order.order_id
    
    async def cancel_order(self, order_id: str) -> bool:
        """
        Request to cancel an order
        Returns True if cancel request was submitted, False if order doesn't exist or is already inactive
        """
        if order_id not in self.orders or order_id not in self.active_orders:
            logger.warning(f"Attempted to cancel non-existent or inactive order: {order_id}")
            return False
        
        order = self.orders[order_id]
        
        # Only cancel if in a cancellable state
        if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            logger.warning(f"Order {order_id} with status {order.status} cannot be cancelled")
            return False
        
        # Update order status
        order.status = OrderStatus.PENDING_CANCEL
        order.updated_at = datetime.utcnow()
        
        # Save order state change
        self._save_order_history(order)
        
        # Publish cancel request event
        await self.event_processor.publish(Event(
            event_type=EventType.ORDER_UPDATE,
            data=order,
            source="order_manager"
        ))
        
        return True
    
    async def modify_order(self, order_id: str, 
                          price: Optional[float] = None, 
                          quantity: Optional[float] = None) -> bool:
        """
        Request to modify an order's price or quantity
        Returns True if modify request was submitted
        """
        if order_id not in self.orders or order_id not in self.active_orders:
            logger.warning(f"Attempted to modify non-existent or inactive order: {order_id}")
            return False
        
        order = self.orders[order_id]
        
        # Only modify if in a modifiable state
        if order.status not in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
            logger.warning(f"Order {order_id} with status {order.status} cannot be modified")
            return False
        
        # Create a new order for the modification
        modified_order = Order.from_dict(order.to_dict())
        
        # Update fields
        if price is not None:
            modified_order.price = price
        if quantity is not None:
            # Can only increase quantity for partially filled orders
            if order.status == OrderStatus.PARTIALLY_FILLED and quantity < order.filled_quantity:
                logger.warning(f"Cannot reduce quantity below filled amount for order {order_id}")
                return False
            modified_order.quantity = quantity
        
        modified_order.updated_at = datetime.utcnow()
        
        # Save the new state
        self._update_order_state(modified_order)
        
        # Publish modify request event
        await self.event_processor.publish(Event(
            event_type=EventType.ORDER_UPDATE,
            data=modified_order,
            source="order_manager"
        ))
        
        return True
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get the current state of an order"""
        return self.orders.get(order_id)
    
    def get_order_history(self, order_id: str) -> List[Order]:
        """Get the history of an order's state changes"""
        return self.order_history.get(order_id, [])
    
    def get_trades(self, order_id: str) -> List[Trade]:
        """Get all trades associated with an order"""
        return self.trades.get(order_id, [])
    
    def get_active_orders(self, strategy_id: Optional[str] = None, 
                         instrument_id: Optional[str] = None) -> List[Order]:
        """Get all currently active orders, optionally filtered"""
        result = []
        for order_id in self.active_orders:
            order = self.orders[order_id]
            
            # Apply filters
            if strategy_id and order.strategy_id != strategy_id:
                continue
            if instrument_id and order.instrument_id != instrument_id:
                continue
                
            result.append(order)
        
        return result
    
    async def batch_cancel_orders(self, order_ids: List[str]) -> Dict[str, bool]:
        """Cancel multiple orders at once"""
        results = {}
        for order_id in order_ids:
            results[order_id] = await self.cancel_order(order_id)
        return results
    
    async def cancel_all_orders(self, strategy_id: Optional[str] = None, 
                              instrument_id: Optional[str] = None) -> int:
        """
        Cancel all active orders, optionally filtered by strategy or instrument
        Returns the number of cancellation requests sent
        """
        orders_to_cancel = self.get_active_orders(strategy_id, instrument_id)
        order_ids = [order.order_id for order in orders_to_cancel]
        
        results = await self.batch_cancel_orders(order_ids)
        return sum(1 for success in results.values() if success)
    
    def get_order_statistics(self) -> Dict:
        """Get statistics about orders in the system"""
        active_count = len(self.active_orders)
        total_count = len(self.orders)
        
        status_counts = {}
        for status in OrderStatus:
            status_counts[status.name] = 0
        
        for order in self.orders.values():
            status_counts[order.status.name] += 1
        
        return {
            "active_orders": active_count,
            "total_orders": total_count,
            "by_status": status_counts
        }
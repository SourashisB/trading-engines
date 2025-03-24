import asyncio
import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
import json

from .data_structures import Position, Trade, OrderSide, Event, EventType

logger = logging.getLogger(__name__)


class PositionManager:
    def __init__(self, event_processor):
        self.positions: Dict[str, Position] = {}  # instrument_id -> Position
        self.event_processor = event_processor
        
        # Performance tracking
        self.position_updates_count = 0
        self.last_total_pnl = 0.0
        
        # Register for trade update events
        self.event_processor.add_handler(EventType.TRADE_UPDATE, self._handle_trade_update)
        self.event_processor.add_handler(EventType.MARKET_DATA, self._handle_market_data)
    
    async def _handle_trade_update(self, event: Event):
        """Process trade update events to update positions"""
        trade = event.data
        if not isinstance(trade, Trade):
            logger.error(f"Invalid trade data in event: {event}")
            return
        
        # Get or create position for this instrument
        instrument_id = trade.instrument_id
        if instrument_id not in self.positions:
            self.positions[instrument_id] = Position(instrument_id=instrument_id)
        
        position = self.positions[instrument_id]
        
        # Apply the trade to update the position
        position.apply_trade(trade)
        position.timestamp = datetime.utcnow()
        
        # Track performance
        self.position_updates_count += 1
        
        # Publish position update event
        await self.event_processor.publish(Event(
            event_type=EventType.POSITION_UPDATE,
            data=position,
            source="position_manager"
        ))
    
    async def _handle_market_data(self, event: Event):
        """Process market data events to update position valuations"""
        market_data = event.data
        
        # Extract price information - structure depends on the type of market data
        instrument_id = market_data.instrument_id
        price = None
        
        # Try to extract price based on data type
        if hasattr(market_data, 'data_type'):
            data_type = market_data.data_type
            if data_type.name == 'QUOTE':
                # For quote data, use mid price
                if 'bid' in market_data.data and 'ask' in market_data.data:
                    price = (market_data.data['bid'] + market_data.data['ask']) / 2
            elif data_type.name == 'TRADE':
                # For trade data, use trade price
                if 'price' in market_data.data:
                    price = market_data.data['price']
            elif data_type.name == 'ORDERBOOK':
                # For orderbook data, use mid price from top of book
                if 'bids' in market_data.data and 'asks' in market_data.data:
                    if market_data.data['bids'] and market_data.data['asks']:
                        best_bid = market_data.data['bids'][0]['price']
                        best_ask = market_data.data['asks'][0]['price']
                        price = (best_bid + best_ask) / 2
            elif data_type.name == 'BAR':
                # For bar data, use close price
                if 'close' in market_data.data:
                    price = market_data.data['close']
        
        if price is None or instrument_id not in self.positions:
            return
        
        position = self.positions[instrument_id]
        old_pnl = position.unrealized_pnl
        
        # Update position with new price
        position.update_price(price)
        position.timestamp = datetime.utcnow()
        
        # Only publish significant PnL changes (to reduce event traffic)
        pnl_change = abs(position.unrealized_pnl - old_pnl)
        if pnl_change > 0.01 or pnl_change / max(abs(old_pnl), 0.01) > 0.001:
            await self.event_processor.publish(Event(
                event_type=EventType.POSITION_UPDATE,
                data=position,
                source="position_manager"
            ))
    
    def get_position(self, instrument_id: str) -> Position:
        """Get position for an instrument, creating a new one if it doesn't exist"""
        if instrument_id not in self.positions:
            self.positions[instrument_id] = Position(instrument_id=instrument_id)
        return self.positions[instrument_id]
    
    def get_all_positions(self) -> List[Position]:
        """Get all positions"""
        return list(self.positions.values())
    
    def get_net_position(self) -> float:
        """Get total position value across all instruments"""
        return sum(position.position_value for position in self.positions.values())
    
    def get_pnl_summary(self) -> Dict:
        """Get P&L summary across all positions"""
        realized_pnl = sum(position.realized_pnl for position in self.positions.values())
        unrealized_pnl = sum(position.unrealized_pnl for position in self.positions.values())
        total_pnl = realized_pnl + unrealized_pnl
        
        return {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "total_pnl": total_pnl
        }
    
    async def update_position_price(self, instrument_id: str, price: float):
        """Manually update a position's current price"""
        position = self.get_position(instrument_id)
        old_pnl = position.unrealized_pnl
        
        position.update_price(price)
        position.timestamp = datetime.utcnow()
        
        # Publish position update event
        await self.event_processor.publish(Event(
            event_type=EventType.POSITION_UPDATE,
            data=position,
            source="position_manager"
        ))
    
    async def add_strategy_allocation(self, instrument_id: str, strategy_id: str, quantity: float):
        """Allocate a portion of a position to a specific strategy"""
        position = self.get_position(instrument_id)
        
        # Update strategy allocation
        position.strategy_allocations[strategy_id] = quantity
        
        # Publish position update event
        await self.event_processor.publish(Event(
            event_type=EventType.POSITION_UPDATE,
            data=position,
            source="position_manager"
        ))
    
    def get_strategy_exposure(self, strategy_id: str) -> Dict[str, float]:
        """Get all position quantities allocated to a specific strategy"""
        result = {}
        for instrument_id, position in self.positions.items():
            if strategy_id in position.strategy_allocations:
                result[instrument_id] = position.strategy_allocations[strategy_id]
        return result
    
    def get_position_statistics(self) -> Dict:
        """Get statistics about positions in the system"""
        pnl_summary = self.get_pnl_summary()
        
        long_positions = sum(1 for p in self.positions.values() if p.quantity > 0)
        short_positions = sum(1 for p in self.positions.values() if p.quantity < 0)
        flat_positions = sum(1 for p in self.positions.values() if p.quantity == 0)
        
        largest_long = max((p.position_value for p in self.positions.values() if p.quantity > 0), default=0)
        largest_short = min((p.position_value for p in self.positions.values() if p.quantity < 0), default=0)
        
        return {
            **pnl_summary,
            "position_count": len(self.positions),
            "long_positions": long_positions,
            "short_positions": short_positions,
            "flat_positions": flat_positions,
            "largest_long_value": largest_long,
            "largest_short_value": largest_short,
            "position_updates": self.position_updates_count
        }
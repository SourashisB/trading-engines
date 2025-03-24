from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import uuid
import numpy as np


class MarketDataType(Enum):
    QUOTE = auto()
    TRADE = auto()
    ORDERBOOK = auto()
    BAR = auto()
    INSTRUMENT_INFO = auto()


class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()
    STOP_LIMIT = auto()
    TRAILING_STOP = auto()
    ICEBERG = auto()
    TWAP = auto()
    VWAP = auto()


class OrderSide(Enum):
    BUY = auto()
    SELL = auto()


class OrderStatus(Enum):
    PENDING_NEW = auto()
    NEW = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    PENDING_CANCEL = auto()
    CANCELLED = auto()
    REJECTED = auto()
    EXPIRED = auto()


class TimeInForce(Enum):
    GTC = auto()  # Good Till Cancelled
    IOC = auto()  # Immediate Or Cancel
    FOK = auto()  # Fill Or Kill
    GTD = auto()  # Good Till Date


@dataclass
class MarketData:
    instrument_id: str
    timestamp: datetime
    data_type: MarketDataType
    exchange: str
    data: Dict[str, Any]
    source: str
    sequence_id: Optional[int] = None

    def to_dict(self) -> Dict:
        return {
            "instrument_id": self.instrument_id,
            "timestamp": self.timestamp.isoformat(),
            "data_type": self.data_type.name,
            "exchange": self.exchange,
            "data": self.data,
            "source": self.source,
            "sequence_id": self.sequence_id
        }

    @classmethod
    def from_dict(cls, data_dict: Dict) -> 'MarketData':
        return cls(
            instrument_id=data_dict["instrument_id"],
            timestamp=datetime.fromisoformat(data_dict["timestamp"]),
            data_type=MarketDataType[data_dict["data_type"]],
            exchange=data_dict["exchange"],
            data=data_dict["data"],
            source=data_dict["source"],
            sequence_id=data_dict.get("sequence_id")
        )


@dataclass
class OrderBook:
    instrument_id: str
    timestamp: datetime
    exchange: str
    bids: List[Dict[str, float]] = field(default_factory=list)  # List of {price, size} dicts
    asks: List[Dict[str, float]] = field(default_factory=list)  # List of {price, size} dicts
    
    def mid_price(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0]["price"] + self.asks[0]["price"]) / 2
    
    def spread(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return self.asks[0]["price"] - self.bids[0]["price"]
    
    def liquidity_within_bps(self, bps: float) -> Dict[str, float]:
        """Calculate available liquidity within given basis points of mid price"""
        mid = self.mid_price()
        if mid is None:
            return {"bid_liquidity": 0.0, "ask_liquidity": 0.0}
        
        threshold = mid * bps / 10000  # Convert bps to price
        bid_liquidity = sum(b["size"] for b in self.bids if mid - b["price"] <= threshold)
        ask_liquidity = sum(a["size"] for a in self.asks if a["price"] - mid <= threshold)
        
        return {"bid_liquidity": bid_liquidity, "ask_liquidity": ask_liquidity}
    
    def to_market_data(self) -> MarketData:
        return MarketData(
            instrument_id=self.instrument_id,
            timestamp=self.timestamp,
            data_type=MarketDataType.ORDERBOOK,
            exchange=self.exchange,
            data={"bids": self.bids, "asks": self.asks},
            source=self.exchange
        )


@dataclass
class Order:
    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    instrument_id: str = ""
    order_type: OrderType = OrderType.MARKET
    side: OrderSide = OrderSide.BUY
    quantity: float = 0.0
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    exchange: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    status: OrderStatus = OrderStatus.PENDING_NEW
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    client_order_id: Optional[str] = None
    parent_order_id: Optional[str] = None
    strategy_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    execution_instructions: Dict[str, Any] = field(default_factory=dict)
    expiry_date: Optional[datetime] = None
    
    def remaining_quantity(self) -> float:
        return self.quantity - self.filled_quantity
    
    def is_active(self) -> bool:
        active_statuses = {
            OrderStatus.PENDING_NEW,
            OrderStatus.NEW,
            OrderStatus.PARTIALLY_FILLED
        }
        return self.status in active_statuses
    
    def to_dict(self) -> Dict:
        return {
            "order_id": self.order_id,
            "instrument_id": self.instrument_id,
            "order_type": self.order_type.name,
            "side": self.side.name,
            "quantity": self.quantity,
            "price": self.price,
            "stop_price": self.stop_price,
            "time_in_force": self.time_in_force.name,
            "exchange": self.exchange,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "status": self.status.name,
            "filled_quantity": self.filled_quantity,
            "average_fill_price": self.average_fill_price,
            "client_order_id": self.client_order_id,
            "parent_order_id": self.parent_order_id,
            "strategy_id": self.strategy_id,
            "tags": self.tags,
            "execution_instructions": self.execution_instructions,
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None
        }
    
    @classmethod
    def from_dict(cls, data_dict: Dict) -> 'Order':
        order = cls(
            order_id=data_dict["order_id"],
            instrument_id=data_dict["instrument_id"],
            order_type=OrderType[data_dict["order_type"]],
            side=OrderSide[data_dict["side"]],
            quantity=data_dict["quantity"],
            price=data_dict["price"],
            stop_price=data_dict["stop_price"],
            time_in_force=TimeInForce[data_dict["time_in_force"]],
            exchange=data_dict["exchange"],
            created_at=datetime.fromisoformat(data_dict["created_at"]),
            updated_at=datetime.fromisoformat(data_dict["updated_at"]),
            status=OrderStatus[data_dict["status"]],
            filled_quantity=data_dict["filled_quantity"],
            average_fill_price=data_dict["average_fill_price"],
            client_order_id=data_dict.get("client_order_id"),
            parent_order_id=data_dict.get("parent_order_id"),
            strategy_id=data_dict.get("strategy_id"),
            tags=data_dict.get("tags", {}),
            execution_instructions=data_dict.get("execution_instructions", {}),
        )
        if data_dict.get("expiry_date"):
            order.expiry_date = datetime.fromisoformat(data_dict["expiry_date"])
        return order


@dataclass
class Trade:
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str = ""
    instrument_id: str = ""
    quantity: float = 0.0
    price: float = 0.0
    side: OrderSide = OrderSide.BUY
    timestamp: datetime = field(default_factory=datetime.utcnow)
    exchange: str = ""
    commission: float = 0.0
    commission_currency: str = "USD"
    
    def to_dict(self) -> Dict:
        return {
            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "instrument_id": self.instrument_id,
            "quantity": self.quantity,
            "price": self.price,
            "side": self.side.name,
            "timestamp": self.timestamp.isoformat(),
            "exchange": self.exchange,
            "commission": self.commission,
            "commission_currency": self.commission_currency
        }


@dataclass
class Position:
    instrument_id: str
    quantity: float = 0.0
    average_entry_price: float = 0.0
    current_price: Optional[float] = None
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    position_value: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    open_orders: List[str] = field(default_factory=list)
    strategy_allocations: Dict[str, float] = field(default_factory=dict)
    exchange: Optional[str] = None
    
    def update_price(self, new_price: float) -> None:
        if self.quantity == 0:
            self.unrealized_pnl = 0.0
            self.position_value = 0.0
            self.current_price = new_price
            return
        
        old_unrealized_pnl = self.unrealized_pnl
        self.current_price = new_price
        self.position_value = self.quantity * new_price
        
        # Calculate unrealized P&L
        if self.quantity > 0:  # Long position
            self.unrealized_pnl = self.quantity * (new_price - self.average_entry_price)
        else:  # Short position
            self.unrealized_pnl = self.quantity * (self.average_entry_price - new_price)
    
    def apply_trade(self, trade: Trade) -> None:
        """Update position with a new trade"""
        old_quantity = self.quantity
        old_cost_basis = abs(old_quantity) * self.average_entry_price if old_quantity != 0 else 0
        
        # Update quantity based on trade side
        if trade.side == OrderSide.BUY:
            trade_quantity = trade.quantity
        else:  # SELL
            trade_quantity = -trade.quantity
        
        # Calculate new position
        new_quantity = old_quantity + trade_quantity
        
        # Update realized P&L if crossing zero or reducing position
        if (old_quantity > 0 and trade_quantity < 0) or (old_quantity < 0 and trade_quantity > 0):
            # Closing or reducing position
            if abs(trade_quantity) <= abs(old_quantity):
                # Partial or full closure
                closing_quantity = min(abs(trade_quantity), abs(old_quantity))
                if old_quantity > 0:  # Long position being reduced
                    self.realized_pnl += closing_quantity * (trade.price - self.average_entry_price)
                else:  # Short position being reduced
                    self.realized_pnl += closing_quantity * (self.average_entry_price - trade.price)
            else:
                # Position crosses zero (flips)
                # Realize P&L on the entire old position
                if old_quantity > 0:  # Long to short
                    self.realized_pnl += old_quantity * (trade.price - self.average_entry_price)
                else:  # Short to long
                    self.realized_pnl += abs(old_quantity) * (self.average_entry_price - trade.price)
                
                # Remaining quantity becomes the new position
                new_position_quantity = abs(trade_quantity) - abs(old_quantity)
                new_position_quantity *= 1 if trade_quantity > 0 else -1
                self.average_entry_price = trade.price
                self.quantity = new_position_quantity
                self.update_price(trade.price)
                return
        
        # Update average entry price if increasing position
        if new_quantity != 0:
            # If same direction or from zero, update average price
            if old_quantity == 0 or (old_quantity > 0 and new_quantity > 0) or (old_quantity < 0 and new_quantity < 0):
                new_cost_basis = old_cost_basis + abs(trade_quantity) * trade.price
                self.average_entry_price = new_cost_basis / abs(new_quantity)
        else:
            # Position closed exactly
            self.average_entry_price = 0
        
        self.quantity = new_quantity
        self.update_price(trade.price)
    
    def to_dict(self) -> Dict:
        return {
            "instrument_id": self.instrument_id,
            "quantity": self.quantity,
            "average_entry_price": self.average_entry_price,
            "current_price": self.current_price,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "position_value": self.position_value,
            "timestamp": self.timestamp.isoformat(),
            "open_orders": self.open_orders,
            "strategy_allocations": self.strategy_allocations,
            "exchange": self.exchange
        }


class EventType(Enum):
    MARKET_DATA = auto()
    ORDER_UPDATE = auto()
    TRADE_UPDATE = auto()
    POSITION_UPDATE = auto()
    STRATEGY_SIGNAL = auto()
    RISK_CHECK = auto()
    SYSTEM_EVENT = auto()


@dataclass
class Event:
    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    data: Any = None
    source: str = ""
    target: Optional[str] = None
    sequence_id: Optional[int] = None
    priority: int = 1  # Lower value means higher priority
    
    def to_dict(self) -> Dict:
        result = {
            "event_type": self.event_type.name,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "target": self.target,
            "sequence_id": self.sequence_id,
            "priority": self.priority
        }
        
        # Handle different data types
        if hasattr(self.data, 'to_dict'):
            result["data"] = self.data.to_dict()
        elif isinstance(self.data, dict):
            result["data"] = self.data
        else:
            result["data"] = str(self.data)
            
        return result
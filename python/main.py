import asyncio
import logging
import os
import signal
import sys
import yaml
from datetime import datetime, timedelta
import random

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from engine.trading_engine import TradingEngine, setup_signal_handlers
from engine.data_structures import (
    MarketData, OrderBook, Order, Trade, OrderType, OrderSide, 
    OrderStatus, TimeInForce, EventType, Event, MarketDataType
)

logger = logging.getLogger(__name__)


async def generate_mock_market_data(trading_engine, instruments, duration_seconds=300):
    """Generate mock market data for testing"""
    logger.info(f"Starting mock market data generator for {len(instruments)} instruments")
    
    # Register a mock data source
    await trading_engine.register_data_source("mock_data_generator")
    
    # Initial prices for instruments
    base_prices = {
        instrument: random.uniform(
            100, 10000 if "BTC" in instrument else 1000 if "ETH" in instrument else 200
        )
        for instrument in instruments
    }
    
    # Keep track of sequence IDs
    sequence_ids = {instrument: 0 for instrument in instruments}
    
    # Generate data for the specified duration
    end_time = datetime.utcnow() + timedelta(seconds=duration_seconds)
    
    try:
        while datetime.utcnow() < end_time and trading_engine.running:
            # Select a random instrument
            instrument = random.choice(instruments)
            
            # Get current price and update it
            current_price = base_prices[instrument]
            
            # Generate price movement (random walk with mean reversion)
            price_change_pct = random.normalvariate(0, 0.0005)  # 0.05% standard deviation
            new_price = current_price * (1 + price_change_pct)
            
            # Add mean reversion
            reversion_strength = 0.05  # 5% reversion to starting price
            starting_price = base_prices[instrument]
            new_price = new_price * (1 - reversion_strength) + starting_price * reversion_strength
            
            base_prices[instrument] = new_price
            
            # Increment sequence ID
            sequence_ids[instrument] += 1
            
            # Determine what type of market data to send
            data_type = random.choices(
                [MarketDataType.QUOTE, MarketDataType.ORDERBOOK, MarketDataType.TRADE],
                weights=[0.5, 0.3, 0.2],  # 50% quotes, 30% orderbook, 20% trades
                k=1
            )[0]
            
            # Create market data
            timestamp = datetime.utcnow()
            
            if data_type == MarketDataType.QUOTE:
                # Generate quote data
                bid = new_price * (1 - random.uniform(0.0001, 0.001))  # 1-10 bps spread
                ask = new_price * (1 + random.uniform(0.0001, 0.001))
                
                market_data = MarketData(
                    instrument_id=instrument,
                    timestamp=timestamp,
                    data_type=MarketDataType.QUOTE,
                    exchange="mock_exchange",
                    data={"bid": bid, "ask": ask, "bid_size": random.uniform(0.1, 10), "ask_size": random.uniform(0.1, 10)},
                    source="mock_data_generator",
                    sequence_id=sequence_ids[instrument]
                )
            
            elif data_type == MarketDataType.ORDERBOOK:
                # Generate orderbook data
                mid_price = new_price
                
                # Create bids (sorted descending by price)
                bid_count = random.randint(5, 15)
                bids = []
                for i in range(bid_count):
                    price_delta_pct = random.uniform(0.0001, 0.005) * (i + 1)  # Increase spread for deeper levels
                    bid_price = mid_price * (1 - price_delta_pct)
                    bid_size = random.uniform(0.1, 20) * (1 / (i + 1))  # More liquidity at better prices
                    bids.append({"price": bid_price, "size": bid_size})
                
                # Create asks (sorted ascending by price)
                ask_count = random.randint(5, 15)
                asks = []
                for i in range(ask_count):
                    price_delta_pct = random.uniform(0.0001, 0.005) * (i + 1)
                    ask_price = mid_price * (1 + price_delta_pct)
                    ask_size = random.uniform(0.1, 20) * (1 / (i + 1))
                    asks.append({"price": ask_price, "size": ask_size})
                
                # Create an orderbook object
                orderbook = OrderBook(
                    instrument_id=instrument,
                    timestamp=timestamp,
                    exchange="mock_exchange",
                    bids=bids,
                    asks=asks
                )
                
                # Convert to market data
                market_data = orderbook.to_market_data()
                market_data.sequence_id = sequence_ids[instrument]
                market_data.source = "mock_data_generator"
            
            else:  # MarketDataType.TRADE
                # Generate trade data
                trade_price = new_price * (1 + random.normalvariate(0, 0.0002))  # Slight noise
                trade_size = random.uniform(0.01, 5)
                side = random.choice([OrderSide.BUY, OrderSide.SELL])
                
                market_data = MarketData(
                    instrument_id=instrument,
                    timestamp=timestamp,
                    data_type=MarketDataType.TRADE,
                    exchange="mock_exchange",
                    data={
                        "price": trade_price,
                        "size": trade_size,
                        "side": side.name,
                        "trade_id": f"mock_trade_{instrument}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                    },
                    source="mock_data_generator",
                    sequence_id=sequence_ids[instrument]
                )
            
            # Send the market data to the trading engine
            await trading_engine.process_market_data(market_data)
            
            # Sleep a random amount of time
            await asyncio.sleep(random.uniform(0.001, 0.05))
    
    except asyncio.CancelledError:
        logger.info("Mock market data generator cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in mock market data generator: {e}")
    
    logger.info("Mock market data generator finished")


async def generate_mock_orders(trading_engine, instruments, duration_seconds=300):
    """Generate mock orders for testing"""
    logger.info("Starting mock order generator")
    
    # Register a mock strategy
    await trading_engine.register_strategy("mock_strategy", {
        "name": "Mock Strategy",
        "description": "A strategy that randomly generates orders for testing",
        "author": "System",
        "version": "1.0.0",
        "parameters": {}
    })
    
    # Generate orders for the specified duration
    end_time = datetime.utcnow() + timedelta(seconds=duration_seconds)
    orders_placed = 0
    
    try:
        # Wait a bit for market data to start flowing
        await asyncio.sleep(2)
        
        while datetime.utcnow() < end_time and trading_engine.running:
            # Select a random instrument
            instrument = random.choice(instruments)
            
            # Get current position
            position = trading_engine.get_position(instrument)
            
            # Decide order side (tend toward mean reversion)
            if position.quantity > 0:
                # More likely to sell when long
                side = random.choices(
                    [OrderSide.BUY, OrderSide.SELL],
                    weights=[0.3, 0.7],
                    k=1
                )[0]
            elif position.quantity < 0:
                # More likely to buy when short
                side = random.choices(
                    [OrderSide.BUY, OrderSide.SELL],
                    weights=[0.7, 0.3],
                    k=1
                )[0]
            else:
                # Equal chance when flat
                side = random.choice([OrderSide.BUY, OrderSide.SELL])
            
            # Decide order type
            order_type = random.choices(
                [OrderType.MARKET, OrderType.LIMIT],
                weights=[0.3, 0.7],  # 70% limit orders
                k=1
            )[0]
            
            # Determine quantity
            base_quantity = random.uniform(0.1, 1.0 if "BTC" in instrument else 5.0 if "ETH" in instrument else 100.0)
            
            # Create order
            order = Order(
                instrument_id=instrument,
                order_type=order_type,
                side=side,
                quantity=base_quantity,
                time_in_force=TimeInForce.GTC,
                exchange="mock_exchange",
                strategy_id="mock_strategy"
            )
            
            # For limit orders, set a price
            if order_type == OrderType.LIMIT:
                # Get current price from position
                current_price = position.current_price
                if current_price is None:
                    # Skip if we don't have a price yet
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    continue
                
                # Set limit price a bit away from current price
                if side == OrderSide.BUY:
                    # Buy slightly below current price
                    order.price = current_price * (1 - random.uniform(0.001, 0.01))
                else:
                    # Sell slightly above current price
                    order.price = current_price * (1 + random.uniform(0.001, 0.01))
            
            # Submit the order
            async def order_callback(updated_order):
                logger.debug(f"Order callback for {updated_order.order_id}: {updated_order.status}")
            
            try:
                order_id = await trading_engine.submit_order(order, order_callback)
                orders_placed += 1
                logger.info(f"Placed {order.order_type.name} {order.side.name} order for {order.quantity} {instrument} with ID {order_id}")
                
                # Randomly cancel some orders
                if order_type == OrderType.LIMIT and random.random() < 0.3:  # 30% chance to cancel
                    # Wait a bit before cancelling
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                    cancel_result = await trading_engine.cancel_order(order_id)
                    if cancel_result:
                        logger.info(f"Cancelled order {order_id}")
            except Exception as e:
                logger.error(f"Error placing order: {e}")
            
            # Sleep a random amount of time between orders
            await asyncio.sleep(random.uniform(0.5, 3.0))
    
    except asyncio.CancelledError:
        logger.info("Mock order generator cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in mock order generator: {e}")
    
    logger.info(f"Mock order generator finished. Placed {orders_placed} orders")


async def simulate_order_fills(trading_engine, duration_seconds=300):
    """Simulate order fills for the mock orders"""
    logger.info("Starting order fill simulator")
    
    # Generate fills for the specified duration
    end_time = datetime.utcnow() + timedelta(seconds=duration_seconds)
    fills_generated = 0
    
    try:
        # Wait a bit for orders to start flowing
        await asyncio.sleep(5)
        
        while datetime.utcnow() < end_time and trading_engine.running:
            # Get all active orders
            active_orders = trading_engine.order_manager.get_active_orders()
            
            if not active_orders:
                await asyncio.sleep(0.5)
                continue
            
            # Select a random order to fill
            order = random.choice(active_orders)
            
            # Determine how much to fill
            remaining_qty = order.quantity - order.filled_quantity
            if remaining_qty <= 0:
                continue
                
            fill_pct = random.choices(
                [0.25, 0.5, 1.0],  # 25%, 50%, or 100% fill
                weights=[0.3, 0.3, 0.4],  # 40% chance of complete fill
                k=1
            )[0]
            
            fill_qty = remaining_qty * fill_pct
            
            # Determine fill price
            if order.order_type == OrderType.MARKET:
                # Fill at current price with slight slippage
                position = trading_engine.get_position(order.instrument_id)
                if position.current_price is None:
                    continue
                
                base_price = position.current_price
                slippage = random.uniform(0.0001, 0.002)  # 1-20 bps slippage
                
                if order.side == OrderSide.BUY:
                    fill_price = base_price * (1 + slippage)
                else:
                    fill_price = base_price * (1 - slippage)
            
            else:  # LIMIT order
                # Fill at the limit price or better
                if order.price is None:
                    continue
                
                if order.side == OrderSide.BUY:
                    # Fill at or below limit price
                    max_improvement = order.price * 0.001  # Max 10 bps price improvement
                    fill_price = order.price - random.uniform(0, max_improvement)
                else:
                    # Fill at or above limit price
                    max_improvement = order.price * 0.001
                    fill_price = order.price + random.uniform(0, max_improvement)
            
            # Create a trade
            trade = Trade(
                order_id=order.order_id,
                instrument_id=order.instrument_id,
                quantity=fill_qty,
                price=fill_price,
                side=order.side,
                timestamp=datetime.utcnow(),
                exchange=order.exchange,
                commission=fill_qty * fill_price * 0.001  # 10 bps commission
            )
            
            # Submit the trade update
            await trading_engine.event_processor.publish(Event(
                event_type=EventType.TRADE_UPDATE,
                data=trade,
                source="mock_exchange"
            ))
            
            fills_generated += 1
            logger.debug(f"Generated fill for order {order.order_id}: {fill_qty} @ {fill_price}")
            
            # Sleep a random amount of time between fills
            await asyncio.sleep(random.uniform(0.1, 1.0))
    
    except asyncio.CancelledError:
        logger.info("Order fill simulator cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in order fill simulator: {e}")
    
    logger.info(f"Order fill simulator finished. Generated {fills_generated} fills")


async def print_trading_status(trading_engine, interval_seconds=10, duration_seconds=300):
    """Periodically print the status of the trading engine"""
    end_time = datetime.utcnow() + timedelta(seconds=duration_seconds)
    
    try:
        while datetime.utcnow() < end_time and trading_engine.running:
            # Get positions
            positions = trading_engine.get_all_positions()
            pnl_summary = trading_engine.position_manager.get_pnl_summary()
            order_stats = trading_engine.order_manager.get_order_statistics()
            
            # Print status
            print("\n==== Trading Engine Status ====")
            print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Active Orders: {order_stats['active_orders']} of {order_stats['total_orders']}")
            
            print("\n-- Positions --")
            for pos in positions:
                if pos.quantity != 0:
                    print(f"{pos.instrument_id}: {pos.quantity:.4f} @ {pos.average_entry_price:.2f} " +
                          f"Current: {pos.current_price:.2f} P&L: {pos.unrealized_pnl:.2f}")
            
            print("\n-- P&L Summary --")
            print(f"Realized P&L: {pnl_summary['realized_pnl']:.2f}")
            print(f"Unrealized P&L: {pnl_summary['unrealized_pnl']:.2f}")
            print(f"Total P&L: {pnl_summary['total_pnl']:.2f}")
            print("==========================\n")
            
            await asyncio.sleep(interval_seconds)
    
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"Error in status printer: {e}")


async def main():
    
    os.makedirs("logs", exist_ok=True)
    
    
    # Instrument list for testing
    test_instruments = [
        "BTC-USD", "ETH-USD", "AAPL", "MSFT", "AMZN", 
        "ES", "NQ", "GC", "CL"
    ]
    
    # Create and start the trading engine
    config_path = "config/trading_config.yaml"
    trading_engine = TradingEngine(config_path)
    
    # Set up signal handlers
    loop = asyncio.get_running_loop()
    setup_signal_handlers(trading_engine, loop)
    
    try:
        # Start the engine
        await trading_engine.start()
        
        # Run simulations for 5 minutes
        simulation_duration = 300
        
        # Create tasks
        tasks = [
            asyncio.create_task(generate_mock_market_data(trading_engine, test_instruments, simulation_duration)),
            asyncio.create_task(generate_mock_orders(trading_engine, test_instruments, simulation_duration)),
            asyncio.create_task(simulate_order_fills(trading_engine, simulation_duration)),
            asyncio.create_task(print_trading_status(trading_engine, 10, simulation_duration))
        ]
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        
        # Stop the engine
        await trading_engine.stop()
        
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
        await trading_engine.stop()
    except Exception as e:
        logger.exception(f"Error in main: {e}")
        await trading_engine.stop()


if __name__ == "__main__":
    import time
    time.sleep(1)  # Small delay to let logging initialize
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt received, exiting...")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
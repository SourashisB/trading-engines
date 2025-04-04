use chrono::{DateTime, Utc};
use rand::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, Write};
use uuid::Uuid;

// ===== DATA STRUCTURES =====

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OrderType::Market => write!(f, "MARKET"),
            OrderType::Limit => write!(f, "LIMIT"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    id: String,
    symbol: String,
    side: Side,
    order_type: OrderType,
    quantity: u32,
    price: Option<f64>,
    timestamp: DateTime<Utc>,
}

impl Order {
    pub fn new(
        symbol: String,
        side: Side,
        order_type: OrderType,
        quantity: u32,
        price: Option<f64>,
    ) -> Self {
        Order {
            id: Uuid::new_v4().to_string(),
            symbol,
            side,
            order_type,
            quantity,
            price,
            timestamp: Utc::now(),
        }
    }
}

impl fmt::Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Order[{}]: {} {} {} x {} @ {}",
            self.id,
            self.symbol,
            self.side,
            self.order_type,
            self.quantity,
            self.price
                .map_or("MARKET".to_string(), |p| format!("{:.2}", p))
        )
    }
}

#[derive(Debug, Clone)]
pub struct Trade {
    id: String,
    symbol: String,
    buyer_order_id: String,
    seller_order_id: String,
    quantity: u32,
    price: f64,
    timestamp: DateTime<Utc>,
}

impl fmt::Display for Trade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Trade[{}]: {} x {} @ {:.2} (Buy: {}, Sell: {})",
            self.id, self.symbol, self.quantity, self.price, self.buyer_order_id, self.seller_order_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct MarketData {
    symbol: String,
    bid: f64,
    ask: f64,
    last_price: f64,
    timestamp: DateTime<Utc>,
}

impl fmt::Display for MarketData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: Bid: {:.2}, Ask: {:.2}, Last: {:.2}",
            self.symbol, self.bid, self.ask, self.last_price
        )
    }
}

// ===== ORDER BOOK =====

#[derive(Debug)]
pub struct OrderBook {
    symbol: String,
    bids: VecDeque<Order>, // Sorted in descending order by price
    asks: VecDeque<Order>, // Sorted in ascending order by price
    trades: Vec<Trade>,
    market_data: MarketData,
}

impl OrderBook {
    pub fn new(symbol: String, initial_price: f64) -> Self {
        OrderBook {
            symbol: symbol.clone(),
            bids: VecDeque::new(),
            asks: VecDeque::new(),
            trades: Vec::new(),
            market_data: MarketData {
                symbol,
                bid: initial_price * 0.99,
                ask: initial_price * 1.01,
                last_price: initial_price,
                timestamp: Utc::now(),
            },
        }
    }

    pub fn add_order(&mut self, mut order: Order) -> Vec<Trade> {
        // For market orders, set the price to ensure best execution
        if order.order_type == OrderType::Market {
            match order.side {
                Side::Buy => order.price = Some(f64::MAX), // Will match with the lowest ask
                Side::Sell => order.price = Some(0.0),     // Will match with the highest bid
            }
        }

        let mut trades = Vec::new();

        // Try to match the order
        match order.side {
            Side::Buy => {
                while order.quantity > 0 && !self.asks.is_empty() {
                    if let Some(ask) = self.asks.front() {
                        // For limit orders, check if the price is acceptable
                        if order.order_type == OrderType::Limit 
                            && order.price.unwrap() < ask.price.unwrap() {
                            break;
                        }

                        let trade_quantity = std::cmp::min(order.quantity, ask.quantity);
                        let trade_price = ask.price.unwrap();

                        // Create a trade
                        let trade = Trade {
                            id: Uuid::new_v4().to_string(),
                            symbol: self.symbol.clone(),
                            buyer_order_id: order.id.clone(),
                            seller_order_id: ask.id.clone(),
                            quantity: trade_quantity,
                            price: trade_price,
                            timestamp: Utc::now(),
                        };

                        trades.push(trade.clone());
                        self.trades.push(trade);

                        // Update market data
                        self.market_data.last_price = trade_price;
                        self.market_data.timestamp = Utc::now();

                        // Update order quantity
                        order.quantity -= trade_quantity;

                        // Update the ask
                        let mut ask = self.asks.pop_front().unwrap();
                        ask.quantity -= trade_quantity;

                        // If the ask still has quantity, put it back
                        if ask.quantity > 0 {
                            self.asks.push_front(ask);
                        }
                    }
                }

                // If the order is not fully filled, add it to the book
                if order.quantity > 0 && order.order_type == OrderType::Limit {
                    self.insert_bid(order);
                    self.update_market_data();
                }
            }
            Side::Sell => {
                while order.quantity > 0 && !self.bids.is_empty() {
                    if let Some(bid) = self.bids.front() {
                        // For limit orders, check if the price is acceptable
                        if order.order_type == OrderType::Limit 
                            && order.price.unwrap() > bid.price.unwrap() {
                            break;
                        }

                        let trade_quantity = std::cmp::min(order.quantity, bid.quantity);
                        let trade_price = bid.price.unwrap();

                        // Create a trade
                        let trade = Trade {
                            id: Uuid::new_v4().to_string(),
                            symbol: self.symbol.clone(),
                            buyer_order_id: bid.id.clone(),
                            seller_order_id: order.id.clone(),
                            quantity: trade_quantity,
                            price: trade_price,
                            timestamp: Utc::now(),
                        };

                        trades.push(trade.clone());
                        self.trades.push(trade);

                        // Update market data
                        self.market_data.last_price = trade_price;
                        self.market_data.timestamp = Utc::now();

                        // Update order quantity
                        order.quantity -= trade_quantity;

                        // Update the bid
                        let mut bid = self.bids.pop_front().unwrap();
                        bid.quantity -= trade_quantity;

                        // If the bid still has quantity, put it back
                        if bid.quantity > 0 {
                            self.bids.push_front(bid);
                        }
                    }
                }

                // If the order is not fully filled, add it to the book
                if order.quantity > 0 && order.order_type == OrderType::Limit {
                    self.insert_ask(order);
                    self.update_market_data();
                }
            }
        }

        trades
    }

    fn insert_bid(&mut self, order: Order) {
        let price = order.price.unwrap();
        let mut idx = 0;

        // Find the position to insert (descending order by price)
        while idx < self.bids.len() && self.bids[idx].price.unwrap() > price {
            idx += 1;
        }

        self.bids.insert(idx, order);
    }

    fn insert_ask(&mut self, order: Order) {
        let price = order.price.unwrap();
        let mut idx = 0;

        // Find the position to insert (ascending order by price)
        while idx < self.asks.len() && self.asks[idx].price.unwrap() < price {
            idx += 1;
        }

        self.asks.insert(idx, order);
    }

    fn update_market_data(&mut self) {
        if !self.bids.is_empty() {
            self.market_data.bid = self.bids[0].price.unwrap();
        }
        if !self.asks.is_empty() {
            self.market_data.ask = self.asks[0].price.unwrap();
        }
        self.market_data.timestamp = Utc::now();
    }

    pub fn get_market_data(&self) -> MarketData {
        self.market_data.clone()
    }

    pub fn get_orders(&self) -> (Vec<Order>, Vec<Order>) {
        (
            self.bids.iter().cloned().collect(),
            self.asks.iter().cloned().collect(),
        )
    }

    pub fn get_trades(&self) -> Vec<Trade> {
        self.trades.clone()
    }
}

// ===== TRADING ENGINE =====

pub struct TradingEngine {
    order_books: HashMap<String, OrderBook>,
}

impl TradingEngine {
    pub fn new() -> Self {
        TradingEngine {
            order_books: HashMap::new(),
        }
    }

    pub fn create_market(&mut self, symbol: &str, initial_price: f64) {
        let order_book = OrderBook::new(symbol.to_string(), initial_price);
        self.order_books.insert(symbol.to_string(), order_book);
    }

    pub fn place_order(&mut self, order: Order) -> Result<Vec<Trade>, String> {
        if let Some(order_book) = self.order_books.get_mut(&order.symbol) {
            Ok(order_book.add_order(order))
        } else {
            Err(format!("Market {} not found", order.symbol))
        }
    }

    pub fn get_market_data(&self, symbol: &str) -> Option<MarketData> {
        self.order_books.get(symbol).map(|ob| ob.get_market_data())
    }

    pub fn get_orders(&self, symbol: &str) -> Option<(Vec<Order>, Vec<Order>)> {
        self.order_books.get(symbol).map(|ob| ob.get_orders())
    }

    pub fn get_trades(&self, symbol: &str) -> Option<Vec<Trade>> {
        self.order_books.get(symbol).map(|ob| ob.get_trades())
    }

    pub fn get_symbols(&self) -> Vec<String> {
        self.order_books.keys().cloned().collect()
    }

    // Generate mock market data
    pub fn populate_with_mock_data(&mut self) {
        // Create some markets
        let symbols = vec!["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"];
        let prices = vec![150.0, 2800.0, 300.0, 3500.0, 750.0];

        for (&symbol, &price) in symbols.iter().zip(prices.iter()) {
            self.create_market(symbol, price);
            self.generate_mock_orders(symbol, price);
        }
    }

    fn generate_mock_orders(&mut self, symbol: &str, price: f64) {
        let mut rng = rand::thread_rng();
        
        // Generate some buy orders
        for _ in 0..10 {
            let price_offset = rng.gen_range(-0.05..0.0);
            let order_price = price * (1.0 + price_offset);
            let quantity = rng.gen_range(10..100);
            
            let order = Order::new(
                symbol.to_string(),
                Side::Buy,
                OrderType::Limit,
                quantity,
                Some(order_price),
            );
            
            let _ = self.place_order(order);
        }
        
        // Generate some sell orders
        for _ in 0..10 {
            let price_offset = rng.gen_range(0.0..0.05);
            let order_price = price * (1.0 + price_offset);
            let quantity = rng.gen_range(10..100);
            
            let order = Order::new(
                symbol.to_string(),
                Side::Sell,
                OrderType::Limit,
                quantity,
                Some(order_price),
            );
            
            let _ = self.place_order(order);
        }
    }
}

// ===== CLI =====

fn print_menu() {
    println!("\n=== TRADING SYSTEM CLI ===");
    println!("1. View available markets");
    println!("2. View market data");
    println!("3. View order book");
    println!("4. View recent trades");
    println!("5. Place limit order");
    println!("6. Place market order");
    println!("7. Generate more mock data");
    println!("8. Exit");
    print!("Select an option: ");
    io::stdout().flush().unwrap();
}

fn read_line() -> String {
    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("Failed to read input");
    input.trim().to_string()
}

fn view_markets(engine: &TradingEngine) {
    println!("\n=== AVAILABLE MARKETS ===");
    for symbol in engine.get_symbols() {
        if let Some(market_data) = engine.get_market_data(&symbol) {
            println!("{}", market_data);
        } else {
            println!("{}: No market data available", symbol);
        }
    }
}

fn view_market_data(engine: &TradingEngine) {
    print!("Enter symbol: ");
    io::stdout().flush().unwrap();
    let symbol = read_line();
    
    if let Some(market_data) = engine.get_market_data(&symbol) {
        println!("\n=== MARKET DATA FOR {} ===", symbol);
        println!("{}", market_data);
    } else {
        println!("Market {} not found", symbol);
    }
}

fn view_order_book(engine: &TradingEngine) {
    print!("Enter symbol: ");
    io::stdout().flush().unwrap();
    let symbol = read_line();
    
    if let Some((bids, asks)) = engine.get_orders(&symbol) {
        println!("\n=== ORDER BOOK FOR {} ===", symbol);
        
        println!("BIDS:");
        for bid in bids {
            println!("  {}", bid);
        }
        
        println!("ASKS:");
        for ask in asks {
            println!("  {}", ask);
        }
    } else {
        println!("Market {} not found", symbol);
    }
}

fn view_trades(engine: &TradingEngine) {
    print!("Enter symbol: ");
    io::stdout().flush().unwrap();
    let symbol = read_line();
    
    if let Some(trades) = engine.get_trades(&symbol) {
        println!("\n=== RECENT TRADES FOR {} ===", symbol);
        
        if trades.is_empty() {
            println!("No trades yet");
        } else {
            for trade in trades.iter().rev().take(10) {
                println!("{}", trade);
            }
        }
    } else {
        println!("Market {} not found", symbol);
    }
}

fn place_limit_order(engine: &mut TradingEngine) {
    print!("Enter symbol: ");
    io::stdout().flush().unwrap();
    let symbol = read_line();
    
    print!("Side (buy/sell): ");
    io::stdout().flush().unwrap();
    let side_input = read_line().to_lowercase();
    let side = if side_input == "buy" { Side::Buy } else { Side::Sell };
    
    print!("Quantity: ");
    io::stdout().flush().unwrap();
    let quantity: u32 = read_line().parse().unwrap_or(0);
    
    print!("Limit price: ");
    io::stdout().flush().unwrap();
    let price: f64 = read_line().parse().unwrap_or(0.0);
    
    if quantity == 0 || price == 0.0 {
        println!("Invalid quantity or price");
        return;
    }
    
    let order = Order::new(
        symbol,
        side,
        OrderType::Limit,
        quantity,
        Some(price),
    );
    
    match engine.place_order(order.clone()) {
        Ok(trades) => {
            println!("Order placed: {}", order);
            if !trades.is_empty() {
                println!("Trades executed:");
                for trade in trades {
                    println!("  {}", trade);
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn place_market_order(engine: &mut TradingEngine) {
    print!("Enter symbol: ");
    io::stdout().flush().unwrap();
    let symbol = read_line();
    
    print!("Side (buy/sell): ");
    io::stdout().flush().unwrap();
    let side_input = read_line().to_lowercase();
    let side = if side_input == "buy" { Side::Buy } else { Side::Sell };
    
    print!("Quantity: ");
    io::stdout().flush().unwrap();
    let quantity: u32 = read_line().parse().unwrap_or(0);
    
    if quantity == 0 {
        println!("Invalid quantity");
        return;
    }
    
    let order = Order::new(
        symbol,
        side,
        OrderType::Market,
        quantity,
        None,
    );
    
    match engine.place_order(order.clone()) {
        Ok(trades) => {
            println!("Order placed: {}", order);
            if !trades.is_empty() {
                println!("Trades executed:");
                for trade in trades {
                    println!("  {}", trade);
                }
            } else {
                println!("No trades executed. No matching orders in the book.");
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}

fn generate_more_mock_data(engine: &mut TradingEngine) {
    for symbol in engine.get_symbols() {
        if let Some(market_data) = engine.get_market_data(&symbol) {
            engine.generate_mock_orders(&symbol, market_data.last_price);
        }
    }
    println!("Generated additional mock orders for all markets");
}

fn main() {
    let mut engine = TradingEngine::new();
    engine.populate_with_mock_data();
    
    loop {
        print_menu();
        let choice = read_line();
        
        match choice.as_str() {
            "1" => view_markets(&engine),
            "2" => view_market_data(&engine),
            "3" => view_order_book(&engine),
            "4" => view_trades(&engine),
            "5" => place_limit_order(&mut engine),
            "6" => place_market_order(&mut engine),
            "7" => generate_more_mock_data(&mut engine),
            "8" => {
                println!("Exiting...");
                break;
            }
            _ => println!("Invalid option"),
        }
    }
}
package com.engine.trading_engine;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orderId;
    private String symbol;
    private String type; // BUY or SELL
    private BigDecimal price;
    private BigDecimal quantity;

    private String status; // PENDING, EXECUTED, CANCELED

    @Enumerated(EnumType.STRING)
    private OrderSide orderSide; // BUY or SELL
}


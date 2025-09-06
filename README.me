# Trading Module

## Overview
Using data from the market pipeline project, this module initiates simple trading strategy, places orders on paper venues, and tracks various metrics regarding the portfolio.

## Architecture

- **Configuration:**  
  This project is orchestrated with Docker and deployed on a dedicated DigitalOcean server separate from the data pipeline. Services are containerized for modularity, and Grafana dashboards are exposed through NGINX.  

- **Market Data Consumer:**  
  A Kafka consumer subscribes to pipeline topics and normalizes incoming quotes into a standardized internal schema.  

- **Signal Engine:**  
  Implements simple, deterministic trading strategies that produce trade signals.  

- **Risk Manager:**  
  Enforces strict limits on position size, notional exposure, daily loss, and order frequency. Any violation blocks downstream execution.  

- **Execution Engine:**  
  Routes orders to a paper venue (Alpaca Paper).  Maintains a lightweight OMS state machine to track order lifecycle (`new → accepted → filled/partial → cancelled`).  

- **Portfolio Service:**  
  Tracks open positions, cash balances, and real-time PnL (realized/unrealized). Supports basic risk analytics such as rolling historical or EWMA Value-at-Risk.  

- **Storage:**  
  Orders, fills, positions, and PnL metrics are written to ClickHouse for historical analysis.

- **Observability:**  
  Prometheus collects metrics across all components (latency, order throughput, rejects, PnL, drawdowns), which are visualized through Grafana dashboards and exposed externally via NGINX. 

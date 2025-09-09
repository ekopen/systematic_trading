# Trading Module

## Overview
Using data from the market pipeline project, this module initiates simple trading strategies, places simulated orders, and tracks various metrics regarding the portfolio.

## Architecture

- **Configuration:**  
  This project is orchestrated with Docker and deployed on a dedicated DigitalOcean server separate from the data pipeline. Services are containerized for modularity, and Grafana dashboards are exposed through NGINX.  

- **Data**  
  We use historical data from the market pipeline project to "train" the models, and then make decisions by using a Kafka consumer that is hooked up to the market pipeline's Producer

- **Signals:**  
  Designed using a class based system, multiple strategies are ran concurrently, and produce buy/sell/hold signals on a frequent basis.

- **Execution:** 
  Execution prices are simulated by delaying incoming Kafka prices

- **Risk:**  
  Before execution is finished, there is a check on various risk metrics, where any violation blocks downstream execution.    

- **Portfolio:**  
  Orders and positions are written to ClickHouse for analysis.


## Future Improvements
**Planned Features:**
- Better orchestrate all the strategies. They should be individually turned on and off.
- Increase complexity of trading decision logic to get out of simple buy/hold/sell. Eventually, ML should be used to fuel decisions, with dramatically more flexibility in strategy implementation. Risk considerations should be more in depth.
- Create dashboards and get running on the server.
- Start tracking PNL metrics, risk metrics, etc.
 
**Known Issues:**
-  The Kafka consumer may be inefficient right now, and should be looked into.

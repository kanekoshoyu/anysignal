# strategy 5: hyperliquid squeeze predictor

## objective
- quantitatively identify long/short squeeze
- identify chances when there is a good condition for predatory short squeeze chances
- backtrace different short/long squeeze conditions and find if there is any correlation


## data source
| signal                    | type       | source      | purpose                                                       |
| ------------------------- | ---------- | ----------- | ------------------------------------------------------------- |
| crypto L2 orderbook       | scalar/map | hyperliquid | demand/supply balance snapshot                                |
| open interest             | scalar     | hyperliquid | liquidation potential and the curve                           |
| liquidation level/heatmap | scalar/map | hyperliquid | detect potential price at which there is a lot of liquidation |


## hypothesis
- at a "good price" to conduct a long/short squeeze, actions will be taken and OI will drop
- OI curve will be a good indicator for observing long/short squeeze (liquidation)
  - when there is liquidation the OI drops drastically (derivative of OI is the level of liquidation)
- clearing house will clear the order after each iteration of buy/sell (less than a second), so there might be a OI drop (liquidation detection) to orderbook latency issue

# validation
cross check with price, if the OI drops and there is a price change, we validate OI is a good indiicator for squeeze

## conditions to track
- squeeze attack liquidity (in USD) 
- cross exchange comparison of ht (in USD) 
# strategy 2 yield curve

## objective
- find the relationship between yield curve and S&P/QQQ

## data source
| signal                | type   | source     |
| --------------------- | ------ | ---------- |
| t bill yields 4 weeks | scalar | ustreasury |
| t bill yields 8 weeks | scalar | ustreasury |
| QQQ index prices      | scalar | polygonio  |
| S&P500 index prices   | scalar | polygonio  |

### Treasury Bills (T-Bills)
Maturity: Short-term, typically 4, 8, 13, 17, 26, or 52 weeks (up to 1 year).
Yield Offering:
Issued at a discount to face value (no periodic interest payments).
Yield is the difference between the purchase price and the face value received at maturity.
### Treasury Notes (T-Notes)
Maturity: Intermediate-term, ranging from 2 to 10 years (common durations: 2, 3, 5, 7, and 10 years).
Yield Offering:
Pay fixed interest (coupon) every six months.
Yield is determined by periodic coupon payments plus appreciation or depreciation relative to the purchase price.
### Treasury Bonds (T-Bonds)
Maturity: Long-term, typically 20 to 30 years.
Yield Offering:
Pay fixed interest (coupon) every six months.
Generally provide higher yields to compensate for long-term risks (interest rate fluctuations, inflation).
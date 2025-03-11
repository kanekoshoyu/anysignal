# strategy 2 insider trading and stock price relationship

## objective
- find the relationship between insider trading and stock prices in terms of product segments
- study correlation with stock and macro market trends such as comparison with net weekly sell/buy

## data source
| signal                                  | type   | source    |
| --------------------------------------- | ------ | --------- |
| sec form 4 filing specific to a company | vector | secapi    |
| sec form 4 filing daily net sum         | vector | secapi    |
| sec form 4 filing weekly net sum        | vector | secapi    |
| stock prices                            | scalar | polygonio |

### Form 3 (Initial Statement of Beneficial Ownership)
Purpose:
Reports initial ownership of securities by corporate insiders (directors, officers, or beneficial owners owning more than 10% of a company's stock).
Timing:
Must be filed within 10 days after becoming an insider or when the company first becomes publicly registered.
### Form 4 (Statement of Changes in Beneficial Ownership)
Purpose:
Reports changes in ownership (purchases, sales, grants, or exercises of options) by insiders.
Timing:
Must be filed within 2 business days after a transaction has occurred.
### Form 5 (Annual Statement of Beneficial Ownership)
Purpose:
Annual report that covers transactions exempt from immediate reporting requirements under Form 4 (such as smaller or deferred transactions), or transactions that insiders failed to report during the year.
Timing:
Filed annually within 45 days after the company's fiscal year-end.
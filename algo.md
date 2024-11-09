# NSE 200 winner algorithm

## General description
1. Sort top 20 from NSE 200 by previous 12 months returns, call them A20.
2. Invest in A20.
3. Wait 1 month.
4. Sort top 20 from NSE 200 by previous 12 months returns, call them B20.
5. Invest in B20.
6. Sell those from A20 which have moved out of top 40 in NSE 200, sorted by previous 12 months returns.

## Algorithm
1. Get NSE 200 symbols list, call it C200.
2. Sort C200 by previous 12 months returns decreasing.
3. select top 40 (T40) and top 20 (T20) from it.
4. Open portfolio.csv, which contains all symbols held currently, call it HXX.
5. For each symbol S in HXX
    1. Search S in T40
    2. If not found, add to sell list
6. Add T20 to buy list
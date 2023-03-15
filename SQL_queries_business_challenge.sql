

/*This query contains three different parts each part was written by different authors
Author 2022 DDMBAN Won Seok    
Author 2022 DDMBAN Meenakshi  
Author 2022 DDMBAN Tatsuya    
Author 2022 DDMBAN Catherine */



/*query 1*/
/*********************************************************************/
/*******       Author 2022 DDMBAN Tatsuya    
				CO-Author 2022 DDMBAN Meenakshi         *************/
/*********************************************************************/

USE invest;

-- Get the random client to pick the target clients
-- Client_id: 497,539,684,740
Select *
FROM customer_details
ORDER BY RAND()
LIMIT 4;

-- Customer's portfolio analysis by each customer
-- Total asset, each securites's proportion of each clients by security type, major asset class, minor asset class
-- And we can assume the client's investment style and preference from the this query
SELECT customer_id, full_name, hc.account_id, security_name, hc.ticker,ROUND(hc.value,2) as value, quantity, ROUND(value * quantity,2) as sum_security_value, ROUND(value * quantity / sum(hc.value * quantity) OVER(PARTITION BY customer_id)*100,2) as proportion_of_portfolio, ROUND(sum(hc.value * quantity) OVER(PARTITION BY customer_id),2) AS sum_portfolio,sm.sec_type, ROUND(value * quantity / sum(hc.value * quantity) OVER(PARTITION BY customer_id, sec_type)*100,2) as  proportion_of_sec_type ,ROUND(sum(hc.value * quantity) OVER(PARTITION BY customer_id, sec_type),2) as sum_of_sec_type, sm.sp500_weight,sm.major_asset_class, ROUND(value * quantity / sum(hc.value * quantity) OVER(PARTITION BY customer_id,sm.major_asset_class) *100,2) as proportion_of_major_asset, ROUND(sum(hc.value * quantity) OVER(PARTITION BY customer_id,sm.major_asset_class),2) as sum_of_mojor_asset , sm.minor_asset_class
FROM account_dim as ad
INNER JOIN customer_details as cd
ON client_id = customer_id
INNER JOIN holdings_current as hc
ON ad.account_id = hc.account_id
INNER JOIN security_masterlist AS sm
ON hc.ticker = sm.ticker
WHERE price_type = 'Adjusted'
AND customer_id IN (497,539,684,740)
GROUP BY full_name, hc.account_id, ticker
ORDER BY account_id ASC
;


-- Check the good performance security order by adjusted return/ risk
-- By checking that, we can see the fit security for client along preference
SELECT g.security_name, g.ticker,ROUND(g.adjusted_24,2) as adjusted_return, g.new_major_asset_class, g.sec_type, g.minor_asset_class
FROM(SELECT 
*, 
AVG(t.returns_12) AS avg_annual_returns, 
STD(t.returns_12) AS sigma_12,
AVG(t.returns_12)/STD(t.returns_12) AS adjusted_12,
AVG(t.returns_18) AS avg_18_returns, 
STD(t.returns_18) AS sigma_18,
AVG(t.returns_18)/STD(t.returns_18) AS adjusted_18,
AVG(t.returns_24) AS avg_24_returns,
STD(t.returns_24) AS sigma_24,
AVG(t.returns_24)/STD(t.returns_24) AS adjusted_24

FROM 
(SELECT 
lagged.security_name,
lagged.ticker, 
lagged.value AS p1, 
lagged.p0, 
lagged.new_major_asset_class,
lagged.sec_type,
lagged.minor_asset_class,
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, (lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, (lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM 
(SELECT 
s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
s.sec_type,
s.minor_asset_class,
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24

FROM invest.security_masterlist AS s



INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'
) AS lagged) AS t
GROUP BY t.ticker
ORDER BY adjusted_24 DESC) AS g
;



/* query 2 */
/*********************************************************************/
/*******       Author 2022 DDMBAN WON SEOK HUR   
               CO-Author 2022 DDMBAN Catherine         *************/
/*********************************************************************/


/*****client 740*****/

/* This query shows the total client 740 portfolio's 12,18,24 months expected returns */



-- SUM of returns of client's each assests 12, 18, 24 months avgerage returns 

SELECT SUM(z.portfolio_12_return) AS client_740_portfolio_total_12_return,
 SUM(z.portfolio_18_return) AS client_740_portfolio_total_18_return,
SUM(z.portfolio_24_return)  AS client_740_portfolio_total_24_return

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return

FROM(SELECT *, 
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_annual_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_annual_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_annual_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_annual_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_annual_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_annual_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_annual_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_annual_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_annual_returns * 0.068,3)
WHEN ticker = 'MUB' THEN ROUND(g.avg_annual_returns * 0.089,3)
WHEN ticker = 'BSV' THEN ROUND(g.avg_annual_returns * 0.065,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_annual_returns * 0.092,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_annual_returns * 0.08,3)
END AS portfolio_12_return,
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_18_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_18_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_18_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_18_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_18_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_18_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_18_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_18_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_18_returns * 0.068,3)
WHEN ticker = 'MUB' THEN ROUND(g.avg_18_returns * 0.089,3)
WHEN ticker = 'BSV' THEN ROUND(g.avg_18_returns * 0.065,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_18_returns * 0.092,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_18_returns * 0.08,3)
END AS portfolio_18_return,
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_24_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_24_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_24_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_24_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_24_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_24_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_24_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_24_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_24_returns * 0.068,3)
WHEN ticker = 'MUB' THEN ROUND(g.avg_24_returns * 0.089,3)
WHEN ticker = 'BSV' THEN ROUND(g.avg_24_returns * 0.065,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_24_returns * 0.092,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_24_returns * 0.08,3)
END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT 
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24


/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */
   
FROM 
(SELECT 
lagged.ticker, 
lagged.value AS p1, 
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18,
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM 
(SELECT 
s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  -- normalizing the data 
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18month returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 2year returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24

FROM invest.security_masterlist AS s



INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'  -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have
HAVING t.ticker IN ('IEF', 'BND', 'JPST', 'MUB', 'BSV', 'ICE', 'TGT', 'CDNS', 'GLDM', 'CNBS', 'DBC', 'TOKE', 'VAMO', 'MSVX')
ORDER BY avg_annual_returns DESC) AS g ) AS z



;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

/* This query shows our client's each portfolio's returns, risk and adjusted returns for 12, 18 and 24 months
   Last column of the result shows the 12, 18 and 24 returns based on the assest that our client is holding on to */


SELECT *, 
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_annual_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_annual_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_annual_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_annual_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_annual_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_annual_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_annual_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_annual_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_annual_returns * 0.068,3)
WHEN ticker = 'MUB' THEN ROUND(g.avg_annual_returns * 0.089,3)
WHEN ticker = 'BSV' THEN ROUND(g.avg_annual_returns * 0.065,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_annual_returns * 0.092,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_annual_returns * 0.08,3)
END AS portfolio_annual_return,
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_18_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_18_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_18_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_18_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_18_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_18_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_18_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_18_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_18_returns * 0.068,3)
WHEN ticker = 'MUB' THEN ROUND(g.avg_18_returns * 0.089,3)
WHEN ticker = 'BSV' THEN ROUND(g.avg_18_returns * 0.065,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_18_returns * 0.092,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_18_returns * 0.08,3)
END AS portfolio_18_return,
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_24_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_24_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_24_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_24_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_24_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_24_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_24_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_24_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_24_returns * 0.068,3)
WHEN ticker = 'MUB' THEN ROUND(g.avg_24_returns * 0.089,3)
WHEN ticker = 'BSV' THEN ROUND(g.avg_24_returns * 0.065,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_24_returns * 0.092,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_24_returns * 0.08,3)
END AS portfolio_24_return

FROM(SELECT 
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24

FROM 
(SELECT 
lagged.ticker, 
lagged.value AS p1, 
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18,
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM 
(SELECT 
s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18month returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 2year returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24

FROM invest.security_masterlist AS s



INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'
) AS lagged) AS t
GROUP BY t.ticker
HAVING t.ticker IN ('IEF', 'BND', 'JPST', 'MUB', 'BSV', 'ICE', 'TGT', 'CDNS', 'GLDM', 'CNBS', 'DBC', 'TOKE', 'VAMO', 'MSVX')
ORDER BY avg_annual_returns DESC) AS g 

;





/********client 497*****/

/* This query shows the total client 497 portfolio's 12,18,24 months expected returns */

-- SUM of returns of client's each assests 12, 18, 24 months avgerage returns 

SELECT SUM(z.portfolio_12_return) AS client_497_portfolio_total_12_return, 
SUM(z.portfolio_18_return) AS client_497_portfolio_total_18_return,
SUM(z.portfolio_24_return) AS client_497_portfolio_total_24_return
FROM(SELECT *, 

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_annual_returns * 0.021,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_annual_returns * 0.029,3)
WHEN ticker = 'IUSB' THEN ROUND(g.avg_annual_returns * 0.033,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_annual_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_annual_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_annual_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_annual_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_annual_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_annual_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_annual_returns * 0.057,3)
WHEN ticker = 'GIGB' THEN ROUND(g.avg_annual_returns * 0.046,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_annual_returns * 0.039,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_annual_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_annual_returns * 0.047,3)
WHEN ticker = 'VCIT' THEN ROUND(g.avg_annual_returns * 0.043,3)

END AS portfolio_12_return,

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_18_returns * 0.021,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_18_returns * 0.029,3)
WHEN ticker = 'IUSB' THEN ROUND(g.avg_18_returns * 0.033,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_18_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_18_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_18_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_18_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_18_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_18_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_18_returns * 0.057,3)
WHEN ticker = 'GIGB' THEN ROUND(g.avg_18_returns * 0.046,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_18_returns * 0.039,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_18_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_18_returns * 0.047,3)
WHEN ticker = 'VCIT' THEN ROUND(g.avg_18_returns * 0.043,3)

END AS portfolio_18_return,

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_24_returns * 0.021,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_24_returns * 0.029,3)
WHEN ticker = 'IUSB' THEN ROUND(g.avg_24_returns * 0.033,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_24_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_24_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_24_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_24_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_24_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_24_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_24_returns * 0.057,3)
WHEN ticker = 'GIGB' THEN ROUND(g.avg_24_returns * 0.046,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_24_returns * 0.039,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_24_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_24_returns * 0.047,3)
WHEN ticker = 'VCIT' THEN ROUND(g.avg_24_returns * 0.043,3)

END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24

/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */

FROM (SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- 12 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 24 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'  -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have
HAVING t.ticker IN ('MARB', 'IGSB', 'IUSB', 'EOPS', 'ARB', 'MNA', 'CNBS', 'THCX', 'AEE', 'GS', 'TMO', 'UPS', 'GIGB', 'TIP', 'VGSH', 'VTEB', 'VCIT')
ORDER BY avg_annual_returns DESC) as g) AS z



;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

/* This query shows our client's each portfolio's returns, risk and adjusted returns for 12, 18 and 24 months
   Last column of the result shows the 12, 18 and 24 returns based on the assest that our client is holding on to */

SELECT *, 
CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_annual_returns * 0.021,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_annual_returns * 0.029,3)
WHEN ticker = 'IUSB' THEN ROUND(g.avg_annual_returns * 0.033,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_annual_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_annual_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_annual_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_annual_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_annual_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_annual_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_annual_returns * 0.057,3)
WHEN ticker = 'GIGB' THEN ROUND(g.avg_annual_returns * 0.046,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_annual_returns * 0.039,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_annual_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_annual_returns * 0.047,3)
WHEN ticker = 'VCIT' THEN ROUND(g.avg_annual_returns * 0.043,3)

END AS portfolio_annual_return,
CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_18_returns * 0.021,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_18_returns * 0.029,3)
WHEN ticker = 'IUSB' THEN ROUND(g.avg_18_returns * 0.033,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_18_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_18_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_18_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_18_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_18_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_18_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_18_returns * 0.057,3)
WHEN ticker = 'GIGB' THEN ROUND(g.avg_18_returns * 0.046,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_18_returns * 0.039,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_18_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_18_returns * 0.047,3)
WHEN ticker = 'VCIT' THEN ROUND(g.avg_18_returns * 0.043,3)

END AS portfolio_18_return,

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_24_returns * 0.021,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_24_returns * 0.029,3)
WHEN ticker = 'IUSB' THEN ROUND(g.avg_24_returns * 0.033,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_24_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_24_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_24_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_24_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_24_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_24_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_24_returns * 0.057,3)
WHEN ticker = 'GIGB' THEN ROUND(g.avg_24_returns * 0.046,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_24_returns * 0.039,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_24_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_24_returns * 0.047,3)
WHEN ticker = 'VCIT' THEN ROUND(g.avg_24_returns * 0.043,3)

END AS portfolio_24_return

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24

FROM (SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'
) AS lagged) AS t
GROUP BY t.ticker
HAVING t.ticker IN ('MARB', 'IGSB', 'IUSB', 'EOPS', 'ARB', 'MNA', 'CNBS', 'THCX', 'AEE', 'GS', 'TMO', 'UPS', 'GIGB', 'TIP', 'VGSH', 'VTEB', 'VCIT')
ORDER BY avg_annual_returns DESC) as g

;



/******client 539******/

/* This query shows the total client 539 portfolio's 12,18,24 months expected returns */

-- SUM of returns of client's each assests 12, 18, 24 months avgerage returns 

SELECT SUM(z.portfolio_12_return) AS client_539_portfolio_total_12_return,
SUM(z.portfolio_18_return) AS client_539_portfolio_total_18_return,
SUM(z.portfolio_24_return) AS client_539_portfolio_total_24_return
FROM(SELECT *, 

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return
CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_annual_returns * 0.028,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_annual_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_annual_returns * 0.02,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_annual_returns * 0.019,3)
WHEN ticker = 'KOLD' THEN ROUND(g.avg_annual_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_annual_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_annual_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_annual_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_annual_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_annual_returns * 0.097,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_annual_returns * 0.04,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_annual_returns * 0.04,3)
WHEN ticker = 'TLT' THEN ROUND(g.avg_annual_returns * 0.079,3)


END AS portfolio_12_return,

CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_18_returns * 0.028,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_18_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_18_returns * 0.02,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_18_returns * 0.019,3)
WHEN ticker = 'KOLD' THEN ROUND(g.avg_18_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_18_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_18_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_18_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_18_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_18_returns * 0.097,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_18_returns * 0.04,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_18_returns * 0.04,3)
WHEN ticker = 'TLT' THEN ROUND(g.avg_18_returns * 0.079,3)


END AS portfolio_18_return,

CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_24_returns * 0.028,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_24_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_24_returns * 0.02,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_24_returns * 0.019,3)
WHEN ticker = 'KOLD' THEN ROUND(g.avg_24_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_24_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_24_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_24_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_24_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_24_returns * 0.097,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_24_returns * 0.04,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_24_returns * 0.04,3)
WHEN ticker = 'TLT' THEN ROUND(g.avg_24_returns * 0.079,3)


END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12) ,3)AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24

/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */


FROM (
SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- 12 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 24 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'  -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have
HAVING t.ticker IN ('HDG', 'IGSB', 'FMF', 'EOPS', 'CTA', 'KOLD', 'AAAU', 'DBC', 'DJP', 'VRSK', 'MTD', 'TEL', 'BND', 'VGSH', 'TLT')
ORDER BY avg_annual_returns DESC) as g ) AS z



;


-- ------------------------------------ --

/* This query shows our client's each portfolio's returns, risk and adjusted returns for 12, 18 and 24 months
   Last column of the result shows the 12, 18 and 24 returns based on the assest that our client is holding on to */

SELECT *, 
CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_annual_returns * 0.028,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_annual_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_annual_returns * 0.02,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_annual_returns * 0.019,3)
WHEN ticker = 'KOLD' THEN ROUND(g.avg_annual_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_annual_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_annual_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_annual_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_annual_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_annual_returns * 0.097,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_annual_returns * 0.04,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_annual_returns * 0.04,3)
WHEN ticker = 'TLT' THEN ROUND(g.avg_annual_returns * 0.079,3)


END AS portfolio_annual_return,

CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_18_returns * 0.028,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_18_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_18_returns * 0.02,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_18_returns * 0.019,3)
WHEN ticker = 'KOLD' THEN ROUND(g.avg_18_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_18_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_18_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_18_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_18_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_18_returns * 0.097,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_18_returns * 0.04,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_18_returns * 0.04,3)
WHEN ticker = 'TLT' THEN ROUND(g.avg_18_returns * 0.079,3)


END AS portfolio_18_return,

CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_24_returns * 0.028,3)
WHEN ticker = 'IGSB' THEN ROUND(g.avg_24_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_24_returns * 0.02,3)
WHEN ticker = 'EOPS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_24_returns * 0.019,3)
WHEN ticker = 'KOLD' THEN ROUND(g.avg_24_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_24_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_24_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_24_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_24_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_24_returns * 0.097,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_24_returns * 0.04,3)
WHEN ticker = 'VGSH' THEN ROUND(g.avg_24_returns * 0.04,3)
WHEN ticker = 'TLT' THEN ROUND(g.avg_24_returns * 0.079,3)


END AS portfolio_24_return

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12) ,3)AS avg_annual_returns, 
ROUND(STD(t.returns_12) ,3)AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12) ,3)AS adjusted_12,
ROUND(AVG(t.returns_18) ,3)AS avg_18_returns, 
ROUND(STD(t.returns_18) ,3)AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24) ,3)AS avg_24_returns,
ROUND(STD(t.returns_24) ,3)AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24) ,3)AS adjusted_24

FROM (
SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'
) AS lagged) AS t
GROUP BY t.ticker
HAVING t.ticker IN ('HDG', 'IGSB', 'FMF', 'EOPS', 'CTA', 'KOLD', 'AAAU', 'DBC', 'DJP', 'VRSK', 'MTD', 'TEL', 'BND', 'VGSH', 'TLT')
ORDER BY avg_annual_returns DESC) as g

;





/*****client 684*****/


/* This query shows the total client 684 portfolio's 12,18,24 months expected returns */

-- SUM of returns of client's each assests 12, 18, 24 months avgerage returns 


SELECT SUM(z.portfolio_12_return) AS client_684_portfolio_total_12_return,
 SUM(z.portfolio_18_return) AS client_684_portfolio_total_18_return,
 SUM(z.portfolio_24_return) AS client_684_portfolio_total_24_return
FROM(SELECT *, 

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return
CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.033,3)   
WHEN ticker = 'SHY' THEN ROUND(g.avg_annual_returns * 0.038,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_annual_returns * 0.074,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_annual_returns * 0.108,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_annual_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_annual_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_annual_returns * 0.241,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_annual_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_annual_returns * 0.032,3)
WHEN ticker = 'YOLO' THEN ROUND(g.avg_annual_returns * 0.004,3)
END AS portfolio_12_return,

CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.033,3)   
WHEN ticker = 'SHY' THEN ROUND(g.avg_18_returns * 0.038,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_18_returns * 0.074,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_18_returns * 0.108,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_18_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_18_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_18_returns * 0.241,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_18_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_18_returns * 0.032,3)
WHEN ticker = 'YOLO' THEN ROUND(g.avg_18_returns * 0.004,3)
END AS portfolio_18_return,

CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.033,3)   
WHEN ticker = 'SHY' THEN ROUND(g.avg_24_returns * 0.038,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_24_returns * 0.074,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_24_returns * 0.108,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_24_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_24_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_24_returns * 0.241,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_24_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_24_returns * 0.032,3)
WHEN ticker = 'YOLO' THEN ROUND(g.avg_24_returns * 0.004,3)
END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12) ,3)AS avg_annual_returns, 
ROUND(STD(t.returns_12) ,3)AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24) ,3)AS avg_24_returns,
ROUND(STD(t.returns_24) ,3)AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24) ,3)AS adjusted_24

/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */


FROM (
SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- 12 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 24returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01' -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have
HAVING t.ticker IN ('ARB', 'SHY', 'BND', 'IEF', 'BIL', 'EXPD', 'CAT', 'SPY', 'KRBN', 'YOLO')
ORDER BY avg_annual_returns DESC) as g) AS z

;



-- ------------------------------- --

/* This query shows our client's each portfolio's returns, risk and adjusted returns for 12, 18 and 24 months
   Last column of the result shows the 12, 18 and 24 returns based on the assest that our client is holding on to */
   
SELECT *, 
CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.033,3)   
WHEN ticker = 'SHY' THEN ROUND(g.avg_annual_returns * 0.038,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_annual_returns * 0.074,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_annual_returns * 0.108,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_annual_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_annual_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_annual_returns * 0.241,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_annual_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_annual_returns * 0.032,3)
WHEN ticker = 'YOLO' THEN ROUND(g.avg_annual_returns * 0.004,3)
END AS portfolio_annual_return,

CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.033,3)   
WHEN ticker = 'SHY' THEN ROUND(g.avg_18_returns * 0.038,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_18_returns * 0.074,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_18_returns * 0.108,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_18_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_18_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_18_returns * 0.241,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_18_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_18_returns * 0.032,3)
WHEN ticker = 'YOLO' THEN ROUND(g.avg_18_returns * 0.004,3)
END AS portfolio_18_return,

CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.033,3)   
WHEN ticker = 'SHY' THEN ROUND(g.avg_24_returns * 0.038,3)
WHEN ticker = 'BND' THEN ROUND(g.avg_24_returns * 0.074,3)
WHEN ticker = 'IEF' THEN ROUND(g.avg_24_returns * 0.108,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_24_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_24_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_24_returns * 0.241,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_24_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_24_returns * 0.032,3)
WHEN ticker = 'YOLO' THEN ROUND(g.avg_24_returns * 0.004,3)
END AS portfolio_24_return

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12) ,3)AS avg_annual_returns, 
ROUND(STD(t.returns_12) ,3)AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24) ,3)AS avg_24_returns,
ROUND(STD(t.returns_24) ,3)AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24) ,3)AS adjusted_24

FROM (
SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'
) AS lagged) AS t
GROUP BY t.ticker
HAVING t.ticker IN ('ARB', 'SHY', 'BND', 'IEF', 'BIL', 'EXPD', 'CAT', 'SPY', 'KRBN', 'YOLO')
ORDER BY avg_annual_returns DESC) as g

;


/*query 3*/
/**************************************************************/
 /*Author 2022 DDMBAN Tatsuya    
				CO-Author 2022 DDMBAN Won Seok Hur  */
/**************************************************************/
                
                
/* showing new suggested portfolio's returns for each client */


-- 740

SELECT SUM(z.portfolio_12_return) AS client_740_portfolio_total_12_return,
 SUM(z.portfolio_18_return) AS client_740_portfolio_total_18_return,
SUM(z.portfolio_24_return)  AS client_740_portfolio_total_24_return

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return

FROM(SELECT *, 
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_annual_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_annual_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_annual_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_annual_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_annual_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_annual_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_annual_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_annual_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_annual_returns * 0.068,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_annual_returns * 0.089,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_annual_returns * 0.065,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_annual_returns * 0.092,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_annual_returns * 0.08,3)
END AS portfolio_12_return,
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_18_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_18_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_18_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_18_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_18_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_18_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_18_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_18_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_18_returns * 0.068,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_18_returns * 0.089,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_18_returns * 0.065,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_18_returns * 0.092,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_18_returns * 0.08,3)
END AS portfolio_18_return,
CASE
WHEN ticker = 'DBC' THEN ROUND(g.avg_24_returns * 0.031,3) 
WHEN ticker = 'CDNS' THEN ROUND(g.avg_24_returns * 0.206,3)
WHEN ticker = 'TGT' THEN ROUND(g.avg_24_returns * 0.255,3) 
WHEN ticker = 'CNBS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'VAMO' THEN ROUND(g.avg_24_returns * 0.02,3) 
WHEN ticker = 'ICE' THEN ROUND(g.avg_24_returns * 0.02,3)
WHEN ticker = 'TOKE' THEN ROUND(g.avg_24_returns * 0.002,3)
WHEN ticker = 'MSVX' THEN ROUND(g.avg_24_returns * 0.012,3)
WHEN ticker = 'GLDM' THEN ROUND(g.avg_24_returns * 0.029,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_24_returns * 0.068,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_24_returns * 0.089,3)
WHEN ticker = 'SPY' THEN ROUND(g.avg_24_returns * 0.065,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_24_returns * 0.092,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_24_returns * 0.08,3)
END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT 
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24


/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */
   
FROM 
(SELECT 
lagged.ticker, 
lagged.value AS p1, 
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18,
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM 
(SELECT 
s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  -- normalizing the data 
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- annual returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18month returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 2year returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24

FROM invest.security_masterlist AS s



INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'  -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have -- MUB - VWO BSV- spy BND -schp IEF- vtip
HAVING t.ticker IN ('VTIP', 'SCHP', 'JPST', 'VWO', 'SPY', 'ICE', 'TGT', 'CDNS', 'GLDM', 'CNBS', 'DBC', 'TOKE', 'VAMO', 'MSVX')
ORDER BY avg_annual_returns DESC) AS g ) AS z


;

-- 497

SELECT SUM(z.portfolio_12_return) AS client_497_portfolio_total_12_return, 
SUM(z.portfolio_18_return) AS client_497_portfolio_total_18_return,
SUM(z.portfolio_24_return) AS client_497_portfolio_total_24_return
FROM(SELECT *, 

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_annual_returns * 0.021,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_annual_returns * 0.029,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_annual_returns * 0.033,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_annual_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_annual_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_annual_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_annual_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_annual_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_annual_returns * 0.057,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_annual_returns * 0.046,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_annual_returns * 0.039,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_annual_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_annual_returns * 0.047,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_annual_returns * 0.043,3)

END AS portfolio_12_return,

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_18_returns * 0.021,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_18_returns * 0.029,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_18_returns * 0.033,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_18_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_18_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_18_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_18_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_18_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_18_returns * 0.057,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_18_returns * 0.046,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_18_returns * 0.039,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_18_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_18_returns * 0.047,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_18_returns * 0.043,3)

END AS portfolio_18_return,

CASE
WHEN ticker = 'MARB' THEN ROUND(g.avg_24_returns * 0.021,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_24_returns * 0.029,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_24_returns * 0.033,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.004,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.007,3)
WHEN ticker = 'MNA' THEN ROUND(g.avg_24_returns * 0.014,3)
WHEN ticker = 'CNBS' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'THCX' THEN ROUND(g.avg_24_returns * 0.002,3)
WHEN ticker = 'AEE' THEN ROUND(g.avg_24_returns * 0.038,3)
WHEN ticker = 'GS' THEN ROUND(g.avg_24_returns * 0.331,3)
WHEN ticker = 'TMO' THEN ROUND(g.avg_24_returns * 0.249,3)
WHEN ticker = 'UPS' THEN ROUND(g.avg_24_returns * 0.057,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_24_returns * 0.046,3)
WHEN ticker = 'BIL' THEN ROUND(g.avg_24_returns * 0.039,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_24_returns * 0.037,3)
WHEN ticker = 'VTEB' THEN ROUND(g.avg_24_returns * 0.047,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_24_returns * 0.043,3)

END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24

/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */

FROM (SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- 12 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 24 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'  -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have -- IGSB - vtip   VGSH - jpst SCHB-schb  GIGB-bil  IUSB-vwo  EOPS-arb
HAVING t.ticker IN ('MARB', 'VTIP', 'VWO', 'ARB', 'ARB', 'MNA', 'CNBS', 'THCX', 'AEE', 'GS', 'TMO', 'UPS', 'TIP', 'BIL', 'JPST', 'VTEB', 'SCHP')
ORDER BY avg_annual_returns DESC) as g) AS z ;




-- 539

SELECT SUM(z.portfolio_12_return) AS client_539_portfolio_total_12_return,
SUM(z.portfolio_18_return) AS client_539_portfolio_total_18_return,
SUM(z.portfolio_24_return) AS client_539_portfolio_total_24_return
FROM(SELECT *, 

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return
CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_annual_returns * 0.028,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_annual_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_annual_returns * 0.02,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_annual_returns * 0.019,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_annual_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_annual_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_annual_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_annual_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_annual_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_annual_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_annual_returns * 0.097,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_annual_returns * 0.04,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_annual_returns * 0.04,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_annual_returns * 0.079,3)


END AS portfolio_12_return,

CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_18_returns * 0.028,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_18_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_18_returns * 0.02,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_18_returns * 0.019,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_18_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_18_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_18_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_18_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_18_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_18_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_18_returns * 0.097,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_18_returns * 0.04,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_18_returns * 0.04,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_18_returns * 0.079,3)


END AS portfolio_18_return,

CASE
WHEN ticker = 'HDG' THEN ROUND(g.avg_24_returns * 0.028,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_24_returns * 0.008,3)
WHEN ticker = 'FMF' THEN ROUND(g.avg_24_returns * 0.02,3)
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'CTA' THEN ROUND(g.avg_24_returns * 0.019,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_24_returns * 0.005,3)
WHEN ticker = 'AAAU' THEN ROUND(g.avg_24_returns * 0.003,3)
WHEN ticker = 'DBC' THEN ROUND(g.avg_24_returns * 0.009,3)
WHEN ticker = 'DJP' THEN ROUND(g.avg_24_returns * 0.027,3)
WHEN ticker = 'VRSK' THEN ROUND(g.avg_24_returns * 0.093,3)
WHEN ticker = 'MTD' THEN ROUND(g.avg_24_returns * 0.529,3)
WHEN ticker = 'TEL' THEN ROUND(g.avg_24_returns * 0.097,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_24_returns * 0.04,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_24_returns * 0.04,3)
WHEN ticker = 'TIP' THEN ROUND(g.avg_24_returns * 0.079,3)


END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12),3) AS avg_annual_returns, 
ROUND(STD(t.returns_12),3) AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12) ,3)AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24),3) AS avg_24_returns,
ROUND(STD(t.returns_24),3) AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24),3) AS adjusted_24

/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */


FROM (
SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- 12 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 24 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01'  -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have IGSB-vtip VGSH-jpst BND-schb TLT-tip EOPS-arb KOLD-krbn
HAVING t.ticker IN ('HDG', 'VTIP', 'FMF', 'ARB', 'CTA', 'KRBN', 'AAAU', 'DBC', 'DJP', 'VRSK', 'MTD', 'TEL', 'SCHB', 'JPST', 'TIP')
ORDER BY avg_annual_returns DESC) as g ) AS z ;



;

-- 684


SELECT SUM(z.portfolio_12_return) AS client_684_portfolio_total_12_return,
 SUM(z.portfolio_18_return) AS client_684_portfolio_total_18_return,
 SUM(z.portfolio_24_return) AS client_684_portfolio_total_24_return
FROM(SELECT *, 

-- Considering the client's holding weights of each assest and multiplied to each assest's 12, 18, 24 months return
CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_annual_returns * 0.033,3)   
WHEN ticker = 'SPY' THEN ROUND(g.avg_annual_returns * 0.038,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_annual_returns * 0.074,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_annual_returns * 0.108,3)
WHEN ticker = 'LLY' THEN ROUND(g.avg_annual_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_annual_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_annual_returns * 0.241,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_annual_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_annual_returns * 0.032,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_annual_returns * 0.004,3)
END AS portfolio_12_return,

CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_18_returns * 0.033,3)   
WHEN ticker = 'SPY' THEN ROUND(g.avg_18_returns * 0.038,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_18_returns * 0.074,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_18_returns * 0.108,3)
WHEN ticker = 'LLY' THEN ROUND(g.avg_18_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_18_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_18_returns * 0.241,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_18_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_18_returns * 0.032,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_18_returns * 0.004,3)
END AS portfolio_18_return,

CASE
WHEN ticker = 'ARB' THEN ROUND(g.avg_24_returns * 0.033,3)   
WHEN ticker = 'SPY' THEN ROUND(g.avg_24_returns * 0.038,3)
WHEN ticker = 'VWO' THEN ROUND(g.avg_24_returns * 0.074,3)
WHEN ticker = 'SCHP' THEN ROUND(g.avg_24_returns * 0.108,3)
WHEN ticker = 'LLY' THEN ROUND(g.avg_24_returns * 0.122,3)
WHEN ticker = 'EXPD' THEN ROUND(g.avg_24_returns * 0.096,3)
WHEN ticker = 'CAT' THEN ROUND(g.avg_24_returns * 0.241,3)
WHEN ticker = 'JPST' THEN ROUND(g.avg_24_returns * 0.252,3)
WHEN ticker = 'KRBN' THEN ROUND(g.avg_24_returns * 0.032,3)
WHEN ticker = 'VTIP' THEN ROUND(g.avg_24_returns * 0.004,3)
END AS portfolio_24_return

/* Calculated each 12, 18, 24 months returns and got average returns and the total risk
each asset. The results were group by ticker filtered the assests based on the assests 
our client is holding to using having clause. 
											*/

FROM(SELECT
t.ticker, 
ROUND(AVG(t.returns_12) ,3)AS avg_annual_returns, 
ROUND(STD(t.returns_12) ,3)AS sigma_12,
ROUND(AVG(t.returns_12)/STD(t.returns_12),3) AS adjusted_12,
ROUND(AVG(t.returns_18),3) AS avg_18_returns, 
ROUND(STD(t.returns_18),3) AS sigma_18,
ROUND(AVG(t.returns_18)/STD(t.returns_18),3) AS adjusted_18,
ROUND(AVG(t.returns_24) ,3)AS avg_24_returns,
ROUND(STD(t.returns_24) ,3)AS sigma_24,
ROUND(AVG(t.returns_24)/STD(t.returns_24) ,3)AS adjusted_24

/* Table lagged in the sub-query have p1 and p0 values of each assest labeled with tickers
   There are three p0 values representing 12, 18, 24 months returns of each assests 
   comparing present price to past 12,18,24 months prices       */


FROM (
SELECT 
lagged.ticker, 
lagged.value AS p1,
lagged.p0, 
(lagged.value-lagged.p0)/lagged.p0 AS returns_12,
lagged.p0_18, 
(lagged.value-lagged.p0_18)/lagged.p0_18 AS returns_18,
lagged.p0_24, 
(lagged.value-lagged.p0_24)/lagged.p0_24 AS returns_24

FROM (SELECT s.id,
CASE 
WHEN s.major_asset_class LIKE ('%equ%') THEN 'equity'  
WHEN s.major_asset_class LIKE ('%fix%') THEN 'fixed_income' 
ELSE s.major_asset_class END AS new_major_asset_class,
s.ticker,
s.security_name,
p.date, 
p.value, 
LAG(p.value, 250)OVER( -- 12 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0,
LAG(p.value, 375)OVER( -- 18 returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_18,
LAG(p.value, 500)OVER( -- 24returns
							PARTITION BY p.ticker
                            ORDER BY p.date
                            ) AS p0_24
FROM invest.security_masterlist AS s

INNER JOIN invest.pricing_daily_new AS p ON s.ticker = p.ticker
WHERE price_type = 'Adjusted'
AND p.value IS NOT NULL
AND p.date >= '2020-01-01' -- limiting to recent 2 years data
) AS lagged) AS t
GROUP BY t.ticker -- filtering just the assests that our clients have -- BIL-vtip SHY-jpst BND-vwo IEF-schp
HAVING t.ticker IN ('ARB', 'SPY', 'VWO', 'SCHP', 'LLY', 'EXPD', 'CAT', 'JPST', 'KRBN', 'VTIP')
ORDER BY avg_annual_returns DESC) as g) AS z

;





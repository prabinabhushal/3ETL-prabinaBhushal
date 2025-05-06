Select * from in_network;

SELECT *from provider_table;


CREATE INDEX provider_table_in_network on in_network(provider_group_id);
CREATE INDEX provider_table_in_network on provider_table(provider_group_id);

--1.Retrieve the provider_group_id, billing_code, and negotiated_rate for all procedures in the rate table where 
--the billing_code_type is 'CPT' and the provider has a matching tin in the provider table LIMIT 20.



EXPLAIN ANALYZE SELECT n.provider_group_id, n.billing_code, n.negotiated_rate , p.tin
FROM in_network as n
JOIN provider_table as p 
ON p.provider_group_id = n.provider_group_id
WHERE n.billing_code_type = 'CPT'
LIMIT 20;



--2.List all distinct provider_group_id values from the rate table that have a billing_class of professional
--and include the corresponding npi from the provider table.



SELECT DISTINCT r.provider_group_id, p.npi
FROM in_network AS r
JOIN provider_table AS p
 ON r.provider_group_id = p.provider_group_id
WHERE r.billing_class = 'professional'
LIMIT 10;



--3.Identify provider_group_id values with more than 10 records in the rate table where the billing_code is '99213' , 
--and show the corresponding npi from the provider table.


SELECT n.provider_group_id, n.billing_code ,  p.npi
FROM provider_table p
JOIN in_network n ON p.provider_group_id = n.provider_group_id
WHERE billing_code = '99213'
GROUP BY n.provider_group_id,n.billing_code , p.npi
HAVING count(n.provider_group_id)>10
LIMIT 20;



--4.List the top 3 provider_group_id values with the highest total negotiated_rate for procedures where
--negotiation_type is negotiated, including the tin from the provider table, ordered by total rate descending.

SELECT p.provider_group_id,n.negotiated_type, n.negotiated_rate, sum(n.negotiated_rate) as total_rate, p.tin
FROM provider_table p
JOIN in_network n ON p.provider_group_id = n.provider_group_id
GROUP BY p.provider_group_id, n.negotiated_type, n.negotiated_rate,p.tin 
HAVING n.negotiated_type = 'negotiated'
ORDER BY sum(n.negotiated_rate) DESC
LIMIT 3;


SELECT r.provider_group_id, SUM(r.negotiated_rate) AS total_rate, p.tin
FROM in_network AS r
JOIN provider_table AS p
ON r.provider_group_id = p.provider_group_id
WHERE r.negotiated_type = 'negotiated'
GROUP BY r.provider_group_id, p.tin
ORDER BY total_rate DESC
LIMIT 3;



--5.Calculate the average negotiated_rate for each provider_group_id in the rate table where the service_code
--contains 11, and include the tin from the provider table limit 10.


SELECT r.provider_group_id, AVG(r.negotiated_rate) AS avg_rate, p.tin
FROM in_network AS r
JOIN provider_table AS p
ON r.provider_group_id = p.provider_group_id
WHERE 11 = ANY(r.service_code)
GROUP BY r.provider_group_id, p.tin
LIMIT 10;

select n.provider_group_id,p.tin,avg(negotiated_rate) as avg_nego,
unnest(n.service_code) as serv_code
from   provider_table as p
join in_network n  on p.provider_group_id=n.provider_group_id
where n.service_code::text like '%11%'
group by n.provider_group_id,p.tin,n.service_code
limit 10;

SELECT 
  n.provider_group_id,
  p.tin,
  AVG(n.negotiated_rate) AS avg_nego
FROM provider_table AS p
JOIN in_network n 
  ON p.provider_group_id = n.provider_group_id
JOIN LATERAL UNNEST(n.service_code) AS sc(code) 
  ON code LIKE '%11%'  -- filters codes containing '11'
GROUP BY n.provider_group_id, p.tin
LIMIT 10;





--Question1: How can you use a CTE to calculate the average negotiation rate for each provider group and then
--join this with the provider table to list providers (by NPI and TIN) in groups where the average negotiation rate exceeds 250?

--Hint: Use a CTE to compute the average negotiated rate per provider_group_id. Then, join the CTE with 
--provider_table to get provider details, filtering for groups with an average rate above 250.

WITH avg_rate_per_group AS (
  SELECT provider_group_id, AVG(negotiated_rate) AS avg_rate
  FROM in_network
  GROUP BY provider_group_id
)
SELECT a.provider_group_id, p.npi, p.tin, a.avg_rate
FROM avg_rate_per_group AS a
JOIN provider_table AS p
ON a.provider_group_id = p.provider_group_id
WHERE a.avg_rate > 250;




-- Extra Q Dense rank 
SELECT 
  provider_group_id,
  negotiated_rate,
  MAX(negotiated_rate) OVER (PARTITION BY provider_group_id) AS max_rate,
  DENSE_RANK() OVER (PARTITION BY provider_group_id ORDER BY negotiated_rate DESC) AS dense_rank,
  RANK() OVER (PARTITION BY provider_group_id ORDER BY negotiated_rate DESC) AS rank
FROM in_network
LIMIT 1000;

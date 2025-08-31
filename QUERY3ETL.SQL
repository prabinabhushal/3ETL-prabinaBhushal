SELECT *FROM in_network;
SELECT *FROM provider_table;

--JOINING THE TABLE
SELECT *
FROM provider_table as p
JOIN in_network as n
ON p.provider_group_id = n.provider_group_id
LIMIT 10;


--FILTERING TABLE
SELECT *
FROM provider_table as p
JOIN in_network as n
ON p.provider_group_id = n.provider_group_id
WHERE tin_type = 1
LIMIT 10;

SELECT *
FROM provider_table as p
JOIN in_network as n
ON p.provider_group_id = n.provider_group_id
WHERE tin_type = 2
LIMIT 10;


SELECT p.*,n.billing_class
FROM provider_table as p
JOIN in_network as n
ON p.provider_group_id = n.provider_group_id
WHERE p.tin_type = 2
LIMIT 10;

SELECT *
FROM provider_table as p
JOIN in_network as n
ON p.provider_group_id = n.provider_group_id
WHERE n.billing_code_modifier IS NOT NULL
LIMIT 100;



--CREATE AN INDEX USING SQL

 --Index for join performance
 CREATE INDEX idx_provider_group_id ON provider_table(provider_group_id);

--Index for filtering 
CREATE INDEX idx_tin_type ON provider_table(tin_type);








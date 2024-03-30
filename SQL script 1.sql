-- What are different Index in the data ? 
SELECT DISTINCT Index_identifier
FROM index_table
ORDER BY 1
;
-- Max and Min for each index
select MAX(High) as Max_Price, Min(High) as Min_Price, Index_identifier
FROM index_table
GROUP BY Index_identifier;

-- Max and Min for each index each year
select MAX(High) as Max_Price, Min(High) as Min_Price, Index_identifier, YEAR(DATE) AS 'Year'
FROM index_table
GROUP BY Index_identifier,YEAR(DATE)
ORDER BY 3,4;


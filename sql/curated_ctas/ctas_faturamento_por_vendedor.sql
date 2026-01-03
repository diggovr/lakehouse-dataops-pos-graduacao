CREATE TABLE lakehouse_curated.faturamento_por_vendedor 
WITH ( format = 'PARQUET', parquet_compression = 'SNAPPY', 
external_location = 's3://engenharia-dados-lakehouse/curated/faturamento_por_vendedor/' ) 
AS SELECT vendedor, SUM(qtditens * valorunitario) AS faturamento 
FROM base_dados GROUP BY vendedor;
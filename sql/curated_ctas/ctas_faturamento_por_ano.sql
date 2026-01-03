CREATE TABLE lakehouse_curated.faturamento_por_ano 
WITH ( format = 'PARQUET', parquet_compression = 'SNAPPY', 
external_location = 's3://engenharia-dados-lakehouse/curated/faturamento_por_ano/' ) 
AS SELECT ano, SUM(qtditens * valorunitario) AS faturamento 
FROM base_dados GROUP BY ano;
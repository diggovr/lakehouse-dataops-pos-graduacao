CREATE TABLE lakehouse_curated.qtd_vendas_por_vendedor
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://engenharia-dados-lakehouse/curated/qtd_vendas_por_vendedor/'
) AS
SELECT
  vendedor,
  COUNT(DISTINCT nfe) AS qtd_vendas
FROM base_dados
GROUP BY vendedor;

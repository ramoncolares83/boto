from boto.database import MySQLDatabase

# Parâmetros de conexão
host = "mano.mysql.database.azure.com"
database = "wolfaws"
user = "ramon"
password = "Bareta1083$$$"

# Criar instância da classe MySQLDatabase
db = MySQLDatabase(host, database, user, password)

# Conectar ao banco de dados
db.connect()

# Executar a consulta para obter a maior data de recebimento
query_max_date = "SELECT MAX(Pagamento) FROM wolfaws.trecebimento;"
result_max_date = db.execute_query(query_max_date)
for row in result_max_date:
    parametros_data = row[0]  # Supondo que a data esteja na primeira coluna

# Substituir @DataParametro pela data obtida na consulta
query_tendencia = f"""
SELECT 
    UnidadeNome,
    CONCAT(MONTH('{parametros_data}'), '/', YEAR('{parametros_data}')) AS 1MesAtual,
    SUM(FaturamentoAtual) AS 2ReceitaAtual,
    SUM(FaturamentoMesAnterior) AS 3ReceitaRestanteMesAnterior,
    SUM(FaturamentoAtual + FaturamentoMesAnterior) AS 4TendenciaBaseadoMesAnteriorMaisAtual,
    SUM(FaturamentoRestanteMesAnterior) AS 6RestanteRecebidoAnoAnterior,
    SUM(FaturamentoAtual + FaturamentoRestanteMesAnterior) AS 7TendenciaReceitaAtualMaisAnoAnterior,
    (SELECT SUM(ValorPago)
     FROM wolfaws.trecebimento
     WHERE UnidadeNome = Resultados.UnidadeNome
     AND MONTH(Pagamento) = MONTH(DATE_ADD('{parametros_data}', INTERVAL -1 MONTH))
     AND YEAR(Pagamento) = YEAR(DATE_ADD('{parametros_data}', INTERVAL -1 MONTH))
     GROUP BY UnidadeNome) AS 5ReceitaFinalMesAnterior,
    (SELECT SUM(ValorPago)
     FROM wolfaws.trecebimento
     WHERE UnidadeNome = Resultados.UnidadeNome
     AND MONTH(Pagamento) = MONTH('{parametros_data}')
     AND YEAR(Pagamento) = YEAR(DATE_ADD('{parametros_data}', INTERVAL -1 YEAR))
     GROUP BY UnidadeNome) AS 8ReceitaFinalAnoAnterior
FROM (
    SELECT 
        tr.UnidadeNome,
        (SELECT SUM(ValorPago)
         FROM wolfaws.trecebimento
         WHERE UnidadeNome = tr.UnidadeNome
         AND MONTH(Pagamento) = MONTH('{parametros_data}')
         AND YEAR(Pagamento) = YEAR('{parametros_data}')
         AND Pagamento <= '{parametros_data}') AS FaturamentoAtual,
        (SELECT SUM(ValorPago)
         FROM wolfaws.trecebimento
         WHERE UnidadeNome = tr.UnidadeNome
         AND MONTH(Pagamento) = MONTH(DATE_ADD('{parametros_data}', INTERVAL -1 MONTH))
         AND YEAR(Pagamento) = YEAR(DATE_ADD('{parametros_data}', INTERVAL -1 MONTH))
         AND DAY(Pagamento) > DAY('{parametros_data}')) AS FaturamentoMesAnterior,
        (SELECT SUM(ValorPago)
         FROM wolfaws.trecebimento
         WHERE UnidadeNome = tr.UnidadeNome
         AND Pagamento BETWEEN DATE_ADD(DATE_ADD('{parametros_data}', INTERVAL -1 YEAR), INTERVAL 1 DAY) AND LAST_DAY(DATE_ADD('{parametros_data}', INTERVAL -1 YEAR))
        ) AS FaturamentoRestanteMesAnterior
    FROM wolfaws.trecebimento tr
    GROUP BY tr.UnidadeNome
) AS Resultados
GROUP BY UnidadeNome;
"""

# Executar a consulta complexa
results_tendencia = db.execute_query(query_tendencia)
for row in results_tendencia:
    print(row)

# Fechar conexão
db.close()


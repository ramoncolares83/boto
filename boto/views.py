import pandas as pd
from django.http import HttpResponse
from django.shortcuts import render, redirect
from boto.database import MySQLDatabase
from babel.dates import format_date
import locale
from decouple import config

# Defina a localização para português brasileiro
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
except locale.Error:
    locale.setlocale(locale.LC_ALL, '')  # Usa a localidade padrão do sistema

# Função para obter os dados da tendência
def obter_dados_tendencia():
    host = config('DB_HOST')
    database = config('DB_NAME')
    user = config('DB_USER')
    password = config('DB_PASSWORD')

    db = MySQLDatabase(host, database, user, password)
    db.connect()

    query_max_date = "SELECT DATE_SUB(MAX(Pagamento), INTERVAL 1 DAY) FROM wolfaws.trecebimento;"
    result_max_date = db.execute_query(query_max_date)
    parametros_data = result_max_date[0][0] if result_max_date else None

    if parametros_data:
        query_tendencia = f"""
        SELECT 
            UnidadeNome,
            CONCAT(MONTH('{parametros_data}'), '/', YEAR('{parametros_data}')) AS MesAtual,
            SUM(FaturamentoAtual) AS ReceitaAtual,
            SUM(FaturamentoMesAnterior) AS ReceitaRestanteMesAnterior,
            SUM(FaturamentoAtual + FaturamentoMesAnterior) AS TendenciaBaseadoMesAnteriorMaisAtual,
            SUM(FaturamentoRestanteMesAnterior) AS RestanteRecebidoAnoAnterior,
            SUM(FaturamentoAtual + FaturamentoRestanteMesAnterior) AS TendenciaReceitaAtualMaisAnoAnterior,
            (SELECT SUM(ValorPago)
             FROM wolfaws.trecebimento
             WHERE UnidadeNome = Resultados.UnidadeNome
             AND MONTH(Pagamento) = MONTH(DATE_ADD('{parametros_data}', INTERVAL -1 MONTH))
             AND YEAR(Pagamento) = YEAR(DATE_ADD('{parametros_data}', INTERVAL -1 MONTH))
             GROUP BY UnidadeNome) AS ReceitaFinalMesAnterior,
            (SELECT SUM(ValorPago)
             FROM wolfaws.trecebimento
             WHERE UnidadeNome = Resultados.UnidadeNome
             AND MONTH(Pagamento) = MONTH('{parametros_data}')
             AND YEAR(Pagamento) = YEAR(DATE_ADD('{parametros_data}', INTERVAL -1 YEAR))
             GROUP BY UnidadeNome) AS ReceitaFinalAnoAnterior
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

        results_tendencia = db.execute_query(query_tendencia)
        totais = [sum(tendencia[i] if tendencia[i] is not None else 0 for tendencia in results_tendencia) for i in range(2, 9)]
    else:
        results_tendencia = []
        totais = [0] * 9

    db.close()
    return results_tendencia, parametros_data, totais

# Vista para mostrar a página principal
def boto_view(request):
    mensagem = ""
    if request.method == "POST":
        email = request.POST.get('inputEmail', '')
        palavra_chave = request.POST.get('palavra_chave', '')

        # Consultar o banco de dados para verificar as credenciais
        if validar_credenciais(email, palavra_chave):
            request.session['palavra_chave_valida'] = True
            return redirect('sucesso')
        else:
            mensagem = "Senha ou email incorreto. Tente novamente."

    return render(request, 'boto/index.html', {'mensagem': mensagem})

def validar_credenciais(email, palavra_chave):
    db = MySQLDatabase(config('DB_HOST'), config('DB_NAME'), config('DB_USER'), config('DB_PASSWORD'))
    db.connect()

    # Incluindo a condição WHERE u.UCIdUser = 7
    query = "SELECT UCUserName FROM uctabusers WHERE UCIdUser = 7 AND UCLogin = %s AND senha = %s;"
    parametros = (email, palavra_chave)

    result = db.execute_query(query, parametros)

    db.close()

    # Se houver um resultado, as credenciais são válidas
    return len(result) > 0

# Vista para mostrar a página de sucesso com os dados de tendência
def sucesso_view(request):
    if not request.session.get('palavra_chave_valida'):
        return redirect('index')
    request.session['palavra_chave_valida'] = False

    results_tendencia, parametros_data, totais = obter_dados_tendencia()
    if parametros_data:
        colunas = [
            'UnidadeNome',
            'MesAtual',
            'ReceitaAtual',
            'ReceitaRestanteMesAnterior',
            'TendenciaBaseadoMesAnterior',
            'RestanteRecebidoAnoAnterior',
            'TendenciaBaseadoAnoAnterior',
            'ReceitaFinalMesAnterior',
            'ReceitaFinalAnoAnterior'
        ]
        df = pd.DataFrame(results_tendencia, columns=colunas)

        df['ReceitaAtual'] = df['ReceitaAtual'].apply(lambda x: f'{x:.0f}')
        df['ReceitaRestanteMesAnterior'] = df['ReceitaRestanteMesAnterior'].apply(lambda x: f'{x:.0f}')
        df['TendenciaBaseadoMesAnterior'] = df['TendenciaBaseadoMesAnterior'].apply(lambda x: f'{x:.0f}')
        df['RestanteRecebidoAnoAnterior'] = df['RestanteRecebidoAnoAnterior'].apply(lambda x: f'{x:.0f}')
        df['TendenciaBaseadoAnoAnterior'] = df['TendenciaBaseadoAnoAnterior'].apply(lambda x: f'{x:.0f}')
        df['ReceitaFinalMesAnterior'] = df['ReceitaFinalMesAnterior'].apply(lambda x: f'{x:.0f}')
        df['ReceitaFinalAnoAnterior'] = df['ReceitaFinalAnoAnterior'].apply(lambda x: f'{x:.0f}')

        dados_formatados = df.to_records(index=False).tolist()

        data_formatada = format_date(parametros_data, format='d \'de\' MMMM \'de\' yyyy', locale='pt_BR')

        context = {
            'data_maxima': data_formatada,
            'tendencias': dados_formatados,
            'totais': totais
        }
        return render(request, 'boto/sucesso.html', context)
    else:
        return render(request, 'boto/erro.html')

# Vista para exportar os dados para Excel
def exportar_excel(request):
    results_tendencia, _, _ = obter_dados_tendencia()
    if results_tendencia:
        df = pd.DataFrame(results_tendencia, columns=[
            'UnidadeNome', 'MesAtual', 'ReceitaAtual', 'ReceitaRestanteMesAnterior', 'TendenciaBaseadoMesAnteriorMaisAtual',
            'RestanteRecebidoAnoAnterior', 'TendenciaReceitaAtualMaisAnoAnterior', 'ReceitaFinalMesAnterior', 'ReceitaFinalAnoAnterior'
        ])
        response = HttpResponse(content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="tabelas.xlsx"'

        with pd.ExcelWriter(response, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Tabela1')

        return response
    else:
        return HttpResponse("Erro ao gerar o arquivo Excel", status=400)

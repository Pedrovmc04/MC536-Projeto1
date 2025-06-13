import pandas as pd
import sqlalchemy
import os

# Database configuration parameters
DB_CONFIG = {
    'dbname': 'my_database',  # Replace with your database name
    'user': 'postgres',       # Replace with your username
    'password': 'mypassword', # Replace with your password
    'host': '127.0.0.1',
    'port': '5433'
}

# Função para exportar todos os dados do banco para um único CSV
def export_full_join_to_csv(output_path):
    query = """
    SELECT
        p.id_pais, p.code AS pais_code, p.nome AS pais_nome,
        s.id_subsistema, s.cod_subsistema, s.nome AS subsistema_nome,
        est.id_estado, est.cod_estado, est.nome AS estado_nome,
        se.id_subsistema AS se_id_subsistema, se.id_estado AS se_id_estado,
        ap.id_agente, ap.nome AS agente_nome,
        u.id_usina, u.nome AS usina_nome, u.tipo, u.modalidade_operacao, u.ceg,
        ug.id_unidade, ug.cod_equipamento, ug.nome_unidade, ug.num_unidade,
        ug.data_entrada_teste, ug.data_entrada_operacao, ug.data_desativacao,
        ug.potencia_efetiva, ug.combustivel,
        ae.id AS acesso_eletricidade_id, ae.ano AS acesso_eletricidade_ano, ae.porcentagem AS acesso_eletricidade_pct,
        acl.id AS acesso_combustivel_id, acl.ano AS acesso_combustivel_ano, acl.porcentagem AS acesso_combustivel_pct,
        aer.id AS acesso_energia_renovavel_id, aer.ano AS acesso_energia_renovavel_ano, aer.porcentagem AS acesso_energia_renovavel_pct,
        iel.id AS investimento_id, iel.ano AS investimento_ano, iel.valor_dolar,
        erpc.id AS renovavel_per_capita_id, erpc.ano AS renovavel_per_capita_ano, erpc.geracao_watts,
        idh.id AS idh_id, idh.ano AS idh_ano, idh.indice AS idh_valor
    FROM Pais p
    LEFT JOIN Subsistema s ON s.id_pais = p.id_pais
    LEFT JOIN Subsistema_Estado se ON se.id_subsistema = s.id_subsistema
    LEFT JOIN Estado est ON se.id_estado = est.id_estado
    LEFT JOIN Usina u ON u.id_estado = est.id_estado
    LEFT JOIN Agente_Proprietario ap ON u.id_agente_proprietario = ap.id_agente
    LEFT JOIN Unidade_Geradora ug ON ug.id_usina = u.id_usina
    LEFT JOIN Acesso_Eletricidade ae ON ae.id_pais = p.id_pais
    LEFT JOIN Acesso_Combustivel_Limpo acl ON acl.id_pais = p.id_pais
    LEFT JOIN Acesso_Energia_Renovavel aer ON aer.id_pais = p.id_pais
    LEFT JOIN Investimento_Energia_Limpa iel ON iel.id_pais = p.id_pais
    LEFT JOIN Energia_Renovavel_Per_Capita erpc ON erpc.id_pais = p.id_pais
    LEFT JOIN IDH idh ON idh.id_pais = p.id_pais
    """
    try:
        # Criando a conexão com SQLAlchemy
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        print("Conexão com o banco de dados estabelecida.")

        # Executando a consulta e salvando os dados em um DataFrame
        df = pd.read_sql_query(query, engine)
        print("Consulta executada com sucesso.")

        # Salvando o DataFrame em um arquivo CSV
        os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Garante que o diretório exista
        df.to_csv(output_path, index=False)
        print(f"Arquivo CSV gerado em: {output_path}")

    except Exception as e:
        print(f"Erro ao exportar dados: {e}")

# Caminho para salvar o arquivo CSV
output_csv_path = './query_results/full_database_export.csv'

# Exportando os dados
export_full_join_to_csv(output_csv_path)
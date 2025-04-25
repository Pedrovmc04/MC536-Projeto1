import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from datetime import datetime
import numpy as np

# PostgreSQL connection parameters
DB_PARAMS = {
    'dbname': 'my_database',  # Replace with your database name
    'user': 'postgres',         # Replace with your username
    'password': 'mypassword',     # Replace with your password
    'host': '127.0.0.1',
    'port': '5432'
}

# Path to CSV files
BASE_DIR = '/home/pedrovmc/Downloads/Banco de dados/data'

# Function to connect to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Connected to PostgreSQL database successfully")
        return conn
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

# Function to load CAPACIDADE_GERACAO.csv
def load_capacidade_geracao():
    file_path = os.path.join(BASE_DIR, 'CAPACIDADE_GERACAO.csv')
    try:
        # Read CSV with semicolon delimiter and skip header rows if needed
        df = pd.read_csv(file_path, delimiter=';', encoding='latin-1')
        
        # Replace empty strings and 'NULL' with None
        df = df.replace(['', 'NULL'], None)
        
        # Convert date columns to datetime
        for date_col in ['dat_entradateste', 'dat_entradaoperacao', 'dat_desativacao']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Convert potencia_efetiva to float
        if 'val_potenciaefetiva' in df.columns:
            df['val_potenciaefetiva'] = pd.to_numeric(df['val_potenciaefetiva'], errors='coerce')
        
        return df
    except Exception as e:
        print(f"Error loading CAPACIDADE_GERACAO.csv: {e}")
        return None

# Function to load country data files
def load_country_data(file_name, value_column_name):
    file_path = os.path.join(BASE_DIR, file_name)
    try:
        df = pd.read_csv(file_path)
        
        # Extract relevant columns
        df = df[['Entity', 'Code', 'Year', value_column_name]]
        
        # Rename columns for consistency
        df = df.rename(columns={
            'Entity': 'nome_pais',
            'Code': 'code',
            'Year': 'ano',
            value_column_name: 'valor'
        })
        
        # Convert numeric values
        df['ano'] = pd.to_numeric(df['ano'], errors='coerce')
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        # Remove rows with NaN in code field
        df = df.dropna(subset=['code'])
        
        return df
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        return None

# Function to insert data into Estado table (previously named Regiao in the code)
def insert_estado(conn, df):
    try:
        cursor = conn.cursor()
        
        # Extract unique regions from the dataframe
        regioes = df[['id_estado', 'nom_estado']].drop_duplicates()
        
        for _, row in regioes.iterrows():
            cursor.execute(
                """
                INSERT INTO Estado (nome, cod_estado, administracao)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (row['nom_estado'], row['id_estado'], None)
            )
            
        conn.commit()
        print(f"Inserted {len(regioes)} states")
    except Exception as e:
        print(f"Error inserting into Estado: {e}")
        conn.rollback()

# Function to insert data into Agente_Proprietario table
def insert_agentes(conn, df):
    try:
        cursor = conn.cursor()
        
        # Extract unique agents from the dataframe
        agentes = df['nom_agenteproprietario'].dropna().unique()
        
        for agente in agentes:
            cursor.execute(
                """
                INSERT INTO Agente_Proprietario (nome)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                """,
                (agente,)
            )
            
        conn.commit()
        print(f"Inserted {len(agentes)} agents")
    except Exception as e:
        print(f"Error inserting into Agente_Proprietario: {e}")
        conn.rollback()

# Function to insert data into Usina table
def insert_usinas(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # First, get mappings for foreign keys
        cursor.execute("SELECT id_estado, cod_estado FROM Estado")
        estado_map = {cod: id for id, cod in cursor.fetchall()}
        
        cursor.execute("SELECT id_agente, nome FROM Agente_Proprietario")
        agente_map = {nome: id for id, nome in cursor.fetchall()}
        
        # Group by unique usina
        usinas = df[['nom_usina', 'nom_agenteproprietario', 'dat_entradateste',
                     'dat_entradaoperacao', 'dat_desativacao', 'nom_tipousina',
                     'nom_modalidadeoperacao', 'id_estado']].drop_duplicates()
        
        for _, row in usinas.iterrows():
            id_agente = agente_map.get(row['nom_agenteproprietario'])
            id_estado = estado_map.get(row['id_estado'])
            
            # Handle NaT/None values for date columns
            data_teste = None if pd.isna(row['dat_entradateste']) else row['dat_entradateste']
            data_operacao = None if pd.isna(row['dat_entradaoperacao']) else row['dat_entradaoperacao']
            data_desativacao = None if pd.isna(row['dat_desativacao']) else row['dat_desativacao']
            
            cursor.execute(
                """
                INSERT INTO Usina (nome, id_agente_proprietario, data_entrada_teste,
                                  data_entrada_operacao, data_desativacao, tipo,
                                  modalidade_operacao, id_estado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id_usina
                """,
                (row['nom_usina'], id_agente, data_teste, 
                 data_operacao, data_desativacao,
                 row['nom_tipousina'], row['nom_modalidadeoperacao'], id_estado)
            )
            
            result = cursor.fetchone()
            if result:
                inserted += 1
                
        conn.commit()
        print(f"Inserted {inserted} power plants")
    except Exception as e:
        print(f"Error inserting into Usina: {e}")
        conn.rollback()

# Function to insert data into Unidade_Geradora table
def insert_unidades_geradoras(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Get usina mapping
        cursor.execute("SELECT id_usina, nome FROM Usina")
        usina_map = {nome: id for id, nome in cursor.fetchall()}
        
        # Extract units
        unidades = df[['nom_usina', 'cod_equipamento', 'nom_unidadegeradora',
                        'num_unidadegeradora', 'val_potenciaefetiva', 
                        'nom_combustivel']].drop_duplicates()
        
        for _, row in unidades.iterrows():
            id_usina = usina_map.get(row['nom_usina'])
            
            # Convert num_unidadegeradora to integer safely
            try:
                num_unidade = int(row['num_unidadegeradora'])
            except (ValueError, TypeError):
                # If conversion fails, set to None
                num_unidade = None
            
            if id_usina:
                cursor.execute(
                    """
                    INSERT INTO Unidade_Geradora (cod_equipamento, nome_unidade,
                                               num_unidade, potencia_efetiva,
                                               combustivel, id_usina)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id_unidade
                    """,
                    (row['cod_equipamento'], row['nom_unidadegeradora'],
                     num_unidade, row['val_potenciaefetiva'],
                     row['nom_combustivel'], id_usina)
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
                    
        conn.commit()
        print(f"Inserted {inserted} generating units")
    except Exception as e:
        print(f"Error inserting into Unidade_Geradora: {e}")
        conn.rollback()

# Function to insert countries
def insert_paises(conn, df):
    try:
        cursor = conn.cursor()
        
        # Extract unique countries and ensure code is not NaN
        paises = df[['nome_pais', 'code']].dropna(subset=['code']).drop_duplicates()
        
        inserted = 0
        for _, row in paises.iterrows():
            cursor.execute(
                """
                INSERT INTO Pais (code, nome)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING code
                """,
                (row['code'], row['nome_pais'])
            )
            
            result = cursor.fetchone()
            if result:
                inserted += 1
                
        conn.commit()
        print(f"Inserted {inserted} countries")
    except Exception as e:
        print(f"Error inserting into Pais: {e}")
        conn.rollback()

# Function to insert data into specified table
def insert_country_data(conn, df, table_name):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        for _, row in df_clean.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {table_name} (code_pais, ano, porcentagem)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (row['code'], row['ano'], row['valor'])
            )
            
            result = cursor.fetchone()
            if result:
                inserted += 1
                
        conn.commit()
        print(f"Inserted {inserted} records into {table_name}")
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")
        conn.rollback()

# Function to insert investment data
def insert_investment_data(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        for _, row in df_clean.iterrows():
            cursor.execute(
                """
                INSERT INTO Investimento_Energia_Limpa (code_pais, ano, valor_dolar)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (row['code'], row['ano'], row['valor'])
            )
            
            result = cursor.fetchone()
            if result:
                inserted += 1
                
        conn.commit()
        print(f"Inserted {inserted} investment records")
    except Exception as e:
        print(f"Error inserting into Investimento_Energia_Limpa: {e}")
        conn.rollback()

# Function to insert renewable energy per capita data
def insert_renewable_per_capita(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        for _, row in df_clean.iterrows():
            cursor.execute(
                """
                INSERT INTO Energia_Renovavel_Per_Capita (code_pais, ano, geracao_watts)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (row['code'], row['ano'], row['valor'])
            )
            
            result = cursor.fetchone()
            if result:
                inserted += 1
                
        conn.commit()
        print(f"Inserted {inserted} renewable per capita records")
    except Exception as e:
        print(f"Error inserting into Energia_Renovavel_Per_Capita: {e}")
        conn.rollback()

# Main function to coordinate data loading and insertion
def main():
    # Connect to the database
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Process CAPACIDADE_GERACAO.csv
        print("\nProcessing CAPACIDADE_GERACAO.csv...")
        cap_data = load_capacidade_geracao()
        if cap_data is not None:
            insert_estado(conn, cap_data)
            insert_agentes(conn, cap_data)
            insert_usinas(conn, cap_data)
            insert_unidades_geradoras(conn, cap_data)
        
        # Load and process country data files
        print("\nProcessing clean fuels data...")
        clean_fuels = load_country_data(
            'access-to-clean-fuels-and-technologies-for-cooking.csv',
            'Proportion of population with primary reliance on clean fuels and technologies for cooking (%) - Residence area type: Total'
        )
        if clean_fuels is not None:
            insert_paises(conn, clean_fuels)
            insert_country_data(conn, clean_fuels, 'Acesso_Combustivel_Limpo')
        
        print("\nProcessing investment data...")
        investment = load_country_data(
            'international-finance-clean-energy.csv',
            '7.a.1 - International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems (millions of constant 2021 United States dollars) - EG_IFF_RANDN - All renewables'
        )
        if investment is not None:
            insert_paises(conn, investment)
            insert_investment_data(conn, investment)
        
        print("\nProcessing renewable capacity data...")
        # Fixed column name handling
        try:
            capacity_df = pd.read_csv(os.path.join(BASE_DIR, 'renewable-electricity-generating-capacity-per-capita.csv'))
            # Get the actual column name that contains the capacity data (fourth column)
            capacity_col = capacity_df.columns[3]
            capacity = load_country_data(
                'renewable-electricity-generating-capacity-per-capita.csv',
                capacity_col
            )
            if capacity is not None:
                insert_paises(conn, capacity)
                insert_renewable_per_capita(conn, capacity)
        except Exception as e:
            print(f"Error processing renewable capacity data: {e}")
        
        print("\nProcessing renewable energy share data...")
        energy_share = load_country_data(
            'share-of-final-energy-consumption-from-renewable-sources.csv',
            '7.2.1 - Renewable energy share in the total final energy consumption (%) - EG_FEC_RNEW'
        )
        if energy_share is not None:
            insert_paises(conn, energy_share)
            insert_country_data(conn, energy_share, 'Acesso_Energia_Renovavel')
        
        print("\nProcessing electricity access data...")
        electricity = load_country_data(
            'share-of-the-population-with-access-to-electricity.csv',
            'Access to electricity (% of population)'
        )
        if electricity is not None:
            insert_paises(conn, electricity)
            insert_country_data(conn, electricity, 'Acesso_Eletricidade')
        
    except Exception as e:
        print(f"An error occurred in the main process: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()

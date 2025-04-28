import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from datetime import datetime
import numpy as np
import unicodedata
from config import DB_CONFIG, DATA_DIR

# Use imported configuration parameters
DB_PARAMS = DB_CONFIG
BASE_DIR = DATA_DIR

# Function to connect to PostgreSQL - updated to set encoding
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_client_encoding('UTF8')  # Set connection encoding to UTF-8
        print("Connected to PostgreSQL database successfully")
        return conn
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

# Function to normalize accented text
def normalize_text(text):
    if not isinstance(text, str):
        return text
    # Normalize accented characters properly
    return text.strip()

# Function to load CAPACIDADE_GERACAO.csv - updated for encoding
def load_capacidade_geracao():
    file_path = os.path.join(BASE_DIR, 'CAPACIDADE_GERACAO.csv')
    try:
        # Try multiple encodings to find the correct one
        encodings = ['utf-8', 'latin-1', 'ISO-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                print(f"Attempting to read with encoding: {encoding}")
                # Read CSV with the specified encoding
                df = pd.read_csv(file_path, delimiter=';', encoding=encoding)
                
                # Replace empty strings and 'NULL' with None
                df = df.replace(['', 'NULL'], None)
                
                # Normalize text in all string columns to handle accents properly
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].apply(normalize_text)
                
                # Convert date columns to datetime
                for date_col in ['dat_entradateste', 'dat_entradaoperacao', 'dat_desativacao']:
                    if date_col in df.columns:
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                
                # Convert potencia_efetiva to float
                if 'val_potenciaefetiva' in df.columns:
                    df['val_potenciaefetiva'] = pd.to_numeric(df['val_potenciaefetiva'], errors='coerce')
                
                print(f"Successfully loaded data with encoding: {encoding}")
                return df
                
            except UnicodeDecodeError:
                print(f"Failed to decode with {encoding}, trying next encoding...")
        
        raise Exception("Could not read the file with any of the attempted encodings")
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

# Function to insert regiao data - update to use id_pais instead of code
def insert_regiao(conn, df):
    try:
        cursor = conn.cursor()
        
        # Get the country id for Brazil
        cursor.execute("SELECT id_pais FROM Pais WHERE nome LIKE '%Brazil%' OR nome LIKE '%Brasil%' LIMIT 1")
        brasil_id = cursor.fetchone()
        
        if not brasil_id:
            # If Brazil doesn't exist, insert it
            cursor.execute(
                """
                INSERT INTO Pais (code, nome)
                VALUES (%s, %s)
                RETURNING id_pais
                """,
                ('BRA', 'Brazil')
            )
            brasil_id = cursor.fetchone()
        
        # Extract unique regions from the dataset using id_subsistema and nom_subsistema
        regioes_df = df[['id_subsistema', 'nom_subsistema']].drop_duplicates()
        regiao_ids = {}
        
        # Insert each unique region
        for _, row in regioes_df.iterrows():
            cod_regiao = row['id_subsistema']
            nome = row['nom_subsistema']
            
            # First check if region already exists
            cursor.execute(
                """
                SELECT id_subsistema FROM Subsistema WHERE cod_regiao = %s
                """,
                (cod_regiao,)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Region exists, update if needed
                cursor.execute(
                    """
                    UPDATE Subsistema SET nome = %s WHERE cod_regiao = %s
                    RETURNING id_subsistema
                    """,
                    (nome, cod_regiao)
                )
                regiao_id = cursor.fetchone()[0]
                regiao_ids[nome] = regiao_id
            else:
                # Insert new region
                cursor.execute(
                    """
                    INSERT INTO Subsistema (cod_regiao, nome, id_pais)
                    VALUES (%s, %s, %s)
                    RETURNING id_subsistema
                    """,
                    (cod_regiao, nome, brasil_id[0])
                )
                regiao_id = cursor.fetchone()[0]
                regiao_ids[nome] = regiao_id
        
        conn.commit()
        print(f"Inserted/updated {len(regiao_ids)} subsistemas")
        
        return regiao_ids
    except Exception as e:
        print(f"Error inserting into Subsistema: {e}")
        conn.rollback()
        return {}

# Function to insert data into Estado table
def insert_estado(conn, df):
    try:
        cursor = conn.cursor()
        
        # Extract unique states from the dataframe
        estados = df[['id_estado', 'nom_estado', 'id_subsistema']].drop_duplicates()
        
        # First insert regions and get region IDs
        regiao_ids = insert_regiao(conn, df)
        
        inserted = 0
        for _, row in estados.iterrows():
            estado_cod = row['id_estado']
            
            # Get the subsistema (region) for this state directly from the data
            id_subsistema = row['id_subsistema']
            
            # Find the region id for this subsistema - now using id_subsistema
            cursor.execute(
                """
                SELECT id_subsistema FROM Subsistema WHERE cod_regiao = %s
                """,
                (id_subsistema,)
            )
            result = cursor.fetchone()
            regiao_id = result[0] if result else None
            
            cursor.execute(
                """
                INSERT INTO Estado (nome, cod_estado, id_subsistema)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (row['nom_estado'], estado_cod, regiao_id)
            )
            inserted += 1
            
        conn.commit()
        print(f"Inserted {inserted} states")
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

# Function to insert data into Usina table - updated to handle accented characters
def insert_usinas(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # First, get mappings for foreign keys
        cursor.execute("SELECT id_estado, cod_estado FROM Estado")
        estado_map = {cod: id for id, cod in cursor.fetchall()}
        
        cursor.execute("SELECT id_agente, nome FROM Agente_Proprietario")
        agente_map = {nome: id for id, nome in cursor.fetchall()}
        
        # Group by unique usina - now including the ceg field
        usinas = df[['nom_usina', 'nom_agenteproprietario', 'nom_tipousina',
                     'nom_modalidadeoperacao', 'id_estado', 'ceg']].drop_duplicates()
        
        for _, row in usinas.iterrows():
            id_agente = agente_map.get(row['nom_agenteproprietario'])
            id_estado = estado_map.get(row['id_estado'])
            
            # Ensure proper handling of accented characters
            tipo_usina = row['nom_tipousina'].strip() if isinstance(row['nom_tipousina'], str) else row['nom_tipousina']
            modalidade = row['nom_modalidadeoperacao'].strip() if isinstance(row['nom_modalidadeoperacao'], str) else row['nom_modalidadeoperacao']
            nome_usina = row['nom_usina'].strip() if isinstance(row['nom_usina'], str) else row['nom_usina']
            ceg = row['ceg'].strip() if isinstance(row['ceg'], str) else row['ceg']
            
            cursor.execute(
                """
                INSERT INTO Usina (nome, id_agente_proprietario, tipo,
                                  modalidade_operacao, id_estado, ceg)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id_usina
                """,
                (nome_usina, id_agente, tipo_usina, modalidade, id_estado, ceg)
            )
            
            result = cursor.fetchone()
            if result:
                inserted += 1
                
        conn.commit()
        print(f"Inserted {inserted} power plants")
    except Exception as e:
        print(f"Error inserting into Usina: {e}")
        conn.rollback()

# Function to insert data into Unidade_Geradora table - updated for encoding
def insert_unidades_geradoras(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Get usina mapping
        cursor.execute("SELECT id_usina, nome FROM Usina")
        usina_map = {nome: id for id, nome in cursor.fetchall()}
        
        # Extract units - now including date fields that were moved from Usina
        unidades = df[['nom_usina', 'cod_equipamento', 'nom_unidadegeradora',
                      'num_unidadegeradora', 'dat_entradateste', 'dat_entradaoperacao',
                      'dat_desativacao', 'val_potenciaefetiva', 
                      'nom_combustivel']].drop_duplicates()
        
        for _, row in unidades.iterrows():
            id_usina = usina_map.get(row['nom_usina'])
            
            # Convert num_unidadegeradora to integer safely
            try:
                num_unidade = int(row['num_unidadegeradora'])
            except (ValueError, TypeError):
                # If conversion fails, set to None
                num_unidade = None
            
            # Handle NaT/None values for date columns
            data_teste = None if pd.isna(row['dat_entradateste']) else row['dat_entradateste']
            data_operacao = None if pd.isna(row['dat_entradaoperacao']) else row['dat_entradaoperacao']
            data_desativacao = None if pd.isna(row['dat_desativacao']) else row['dat_desativacao']
            
            # Handle accented characters properly
            combustivel = row['nom_combustivel'].strip() if isinstance(row['nom_combustivel'], str) else row['nom_combustivel']
            nome_unidade = row['nom_unidadegeradora'].strip() if isinstance(row['nom_unidadegeradora'], str) else row['nom_unidadegeradora']
            cod_equip = row['cod_equipamento'].strip() if isinstance(row['cod_equipamento'], str) else row['cod_equipamento']
            
            if id_usina:
                cursor.execute(
                    """
                    INSERT INTO Unidade_Geradora (cod_equipamento, nome_unidade,
                                               num_unidade, data_entrada_teste,
                                               data_entrada_operacao, data_desativacao,
                                               potencia_efetiva, combustivel, id_usina)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id_unidade
                    """,
                    (cod_equip, nome_unidade,
                     num_unidade, data_teste, data_operacao,
                     data_desativacao, row['val_potenciaefetiva'],
                     combustivel, id_usina)
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
                    
        conn.commit()
        print(f"Inserted {inserted} generating units")
    except Exception as e:
        print(f"Error inserting into Unidade_Geradora: {e}")
        conn.rollback()

# Update insert_paises function to return id_pais for each country code
def insert_paises(conn, df):
    try:
        cursor = conn.cursor()
        
        # Extract unique countries and ensure code is not NaN
        paises = df[['nome_pais', 'code']].dropna(subset=['code']).drop_duplicates()
        
        inserted = 0
        country_ids = {}  # Dictionary to store code -> id_pais mapping
        
        for _, row in paises.iterrows():
            # First check if country exists
            cursor.execute(
                """
                SELECT id_pais FROM Pais WHERE code = %s
                """,
                (row['code'],)
            )
            existing = cursor.fetchone()
            
            if existing:
                country_ids[row['code']] = existing[0]
            else:
                cursor.execute(
                    """
                    INSERT INTO Pais (code, nome)
                    VALUES (%s, %s)
                    RETURNING id_pais
                    """,
                    (row['code'], row['nome_pais'])
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
                    country_ids[row['code']] = result[0]
                
        conn.commit()
        print(f"Inserted {inserted} countries")
        return country_ids
    except Exception as e:
        print(f"Error inserting into Pais: {e}")
        conn.rollback()
        return {}

# Update insert_country_data function to use id_pais instead of code_pais
def insert_country_data(conn, df, table_name):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        # Get country_id mapping
        country_ids = {}
        cursor.execute("SELECT id_pais, code FROM Pais")
        for id_pais, code in cursor.fetchall():
            country_ids[code] = id_pais
        
        for _, row in df_clean.iterrows():
            code = row['code']
            id_pais = country_ids.get(code)
            
            if id_pais:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name} (id_pais, ano, porcentagem)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (id_pais, row['ano'], row['valor'])
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
            else:
                print(f"Warning: No country ID found for code {code}")
                
        conn.commit()
        print(f"Inserted {inserted} records into {table_name}")
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")
        conn.rollback()

# Update insert_investment_data function
def insert_investment_data(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        # Get country_id mapping
        country_ids = {}
        cursor.execute("SELECT id_pais, code FROM Pais")
        for id_pais, code in cursor.fetchall():
            country_ids[code] = id_pais
            
        for _, row in df_clean.iterrows():
            code = row['code']
            id_pais = country_ids.get(code)
            
            if id_pais:
                cursor.execute(
                    """
                    INSERT INTO Investimento_Energia_Limpa (id_pais, ano, valor_dolar)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (id_pais, row['ano'], row['valor'])
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
            else:
                print(f"Warning: No country ID found for code {code}")
                
        conn.commit()
        print(f"Inserted {inserted} investment records")
    except Exception as e:
        print(f"Error inserting into Investimento_Energia_Limpa: {e}")
        conn.rollback()

# Update insert_renewable_per_capita function
def insert_renewable_per_capita(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        # Get country_id mapping
        country_ids = {}
        cursor.execute("SELECT id_pais, code FROM Pais")
        for id_pais, code in cursor.fetchall():
            country_ids[code] = id_pais
        
        for _, row in df_clean.iterrows():
            code = row['code']
            id_pais = country_ids.get(code)
            
            if id_pais:
                cursor.execute(
                    """
                    INSERT INTO Energia_Renovavel_Per_Capita (id_pais, ano, geracao_watts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (id_pais, row['ano'], row['valor'])
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
            else:
                print(f"Warning: No country ID found for code {code}")
                
        conn.commit()
        print(f"Inserted {inserted} renewable per capita records")
    except Exception as e:
        print(f"Error inserting into Energia_Renovavel_Per_Capita: {e}")
        conn.rollback()

# Function to load and process HDI data
def load_hdi_data():
    file_path = os.path.join(BASE_DIR, 'HDR23-24_Composite_indices_complete_time_series.csv')
    
    # List of encodings to try
    encodings = ['latin-1', 'ISO-8859-1', 'cp1252', 'utf-8-sig']
    
    for encoding in encodings:
        try:
            print(f"Trying to read HDI data with encoding: {encoding}")
            # Read CSV file with the current encoding
            df = pd.read_csv(file_path, encoding=encoding)
            
            # Get the column names that contain HDI data (columns starting with 'hdi_')
            hdi_columns = [col for col in df.columns if col.startswith('hdi_') and col != 'hdicode']
            
            # Create a list to store the data in the format (country_code, year, value)
            hdi_data = []
            
            # Process each country
            for _, row in df.iterrows():
                iso3 = row['iso3']
                country_name = row['country']
                
                # Process each year's HDI value
                for col in hdi_columns:
                    # Extract the year from the column name (format: hdi_YYYY)
                    year = col.split('_')[1]
                    
                    # Try to convert to integer (handle cases like 'hdi_rank' if they exist)
                    try:
                        year = int(year)
                    except ValueError:
                        continue
                    
                    # Get the HDI value
                    hdi_value = row[col]
                    
                    # Only add if the HDI value is not null
                    if pd.notna(hdi_value):
                        hdi_data.append({
                            'code': iso3,
                            'nome_pais': country_name,
                            'ano': year,
                            'valor': float(hdi_value)
                        })
            
            # Convert list to DataFrame
            hdi_df = pd.DataFrame(hdi_data)
            print(f"Successfully loaded HDI data with encoding: {encoding}")
            return hdi_df
        
        except Exception as e:
            print(f"Failed with encoding {encoding}: {e}")
            continue
    
    # If all encodings fail
    print("Error: Could not load HDI data with any of the attempted encodings")
    return None

# Function to insert HDI data
def insert_hdi_data(conn, df):
    try:
        cursor = conn.cursor()
        inserted = 0
        
        # Filter out rows with NaN codes
        df_clean = df.dropna(subset=['code'])
        
        # Get country_id mapping
        country_ids = {}
        cursor.execute("SELECT id_pais, code FROM Pais")
        for id_pais, code in cursor.fetchall():
            country_ids[code] = id_pais
        
        # Add any missing countries
        new_countries = []
        for _, row in df_clean.iterrows():
            if row['code'] not in country_ids:
                new_countries.append((row['code'], row['nome_pais']))
        
        # Insert new countries if any
        if new_countries:
            new_countries_unique = list(set(new_countries))
            for code, nome in new_countries_unique:
                cursor.execute(
                    """
                    INSERT INTO Pais (code, nome)
                    VALUES (%s, %s)
                    RETURNING id_pais
                    """,
                    (code, nome)
                )
                result = cursor.fetchone()
                if result:
                    country_ids[code] = result[0]
        
        # Now insert HDI data
        for _, row in df_clean.iterrows():
            code = row['code']
            id_pais = country_ids.get(code)
            
            if id_pais:
                cursor.execute(
                    """
                    INSERT INTO IDH (id_pais, ano, indice)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (id_pais, row['ano'], row['valor'])
                )
                
                result = cursor.fetchone()
                if result:
                    inserted += 1
            else:
                print(f"Warning: No country ID found for HDI code {code}")
                
        conn.commit()
        print(f"Inserted {inserted} HDI records")
    except Exception as e:
        print(f"Error inserting into IDH: {e}")
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
            # Insert countries first if needed for regions
            print("\nProcessing countries for regions...")
            electricity = load_country_data(
                'share-of-the-population-with-access-to-electricity.csv',
                'Access to electricity (% of population)'
            )
            if electricity is not None:
                insert_paises(conn, electricity)
            
            # Now insert regions and states
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
        
        print("\nProcessing HDI data...")
        hdi_data = load_hdi_data()
        if hdi_data is not None:
            # Insert countries from HDI data if not already present
            insert_paises(conn, hdi_data)
            # Insert the HDI data
            insert_hdi_data(conn, hdi_data)
        
    except Exception as e:
        print(f"An error occurred in the main process: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()

-- Script to drop all tables in the correct order to respect foreign key constraints

-- Drop tables with foreign keys first
DROP TABLE IF EXISTS IDH CASCADE;
DROP TABLE IF EXISTS Energia_Renovavel_Per_Capita CASCADE;
DROP TABLE IF EXISTS Investimento_Energia_Limpa CASCADE;
DROP TABLE IF EXISTS Acesso_Combustivel_Limpo CASCADE;
DROP TABLE IF EXISTS Acesso_Energia_Renovavel CASCADE;
DROP TABLE IF EXISTS Acesso_Eletricidade CASCADE;
DROP TABLE IF EXISTS Unidade_Geradora CASCADE;
DROP TABLE IF EXISTS Usina CASCADE;
DROP TABLE IF EXISTS Subsistema CASCADE;

-- Drop tables without dependencies last
DROP TABLE IF EXISTS Agente_Proprietario CASCADE;
DROP TABLE IF EXISTS Estado CASCADE;
DROP TABLE IF EXISTS Pais CASCADE;

-- Confirmation message
SELECT 'All tables have been dropped successfully.' as result;

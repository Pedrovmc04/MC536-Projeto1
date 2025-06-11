-- 1. Tabela Pais
CREATE TABLE Pais (
    id_pais SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    nome VARCHAR(100) NOT NULL
);

-- 2. Tabela Subsistema
CREATE TABLE Subsistema (
    id_subsistema SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    id_pais INTEGER REFERENCES Pais(id_pais),
    cod_subsistema VARCHAR(3) UNIQUE NOT NULL
);

-- 3. Tabela Estado
CREATE TABLE Estado (
    id_estado SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    id_subsistema INTEGER,
    cod_estado VARCHAR(2) NOT NULL
);

-- Nova tabela de relacionamento Subsistema_estado
CREATE TABLE Subsistema_estado (
    id_subsistema_estado SERIAL PRIMARY KEY,
    id_subsistema INTEGER REFERENCES Subsistema(id_subsistema),
    id_estado INTEGER REFERENCES Estado(id_estado)
);

-- 4. Tabela Agente_Proprietario
CREATE TABLE Agente_Proprietario (
    id_agente SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL
);

-- 5. Tabela Usina
CREATE TABLE Usina (
    id_usina SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    id_agente_proprietario INTEGER REFERENCES Agente_Proprietario(id_agente),
    tipo VARCHAR(50),
    modalidade_operacao VARCHAR(50),
    id_estado INTEGER REFERENCES Estado(id_estado),
    ceg VARCHAR(50) UNIQUE
);

-- 6. Tabela Unidade_Geradora
CREATE TABLE Unidade_Geradora (
    id_unidade SERIAL PRIMARY KEY,
    cod_equipamento VARCHAR(50) NOT NULL,
    nome_unidade VARCHAR(100),
    num_unidade INTEGER,
    data_entrada_teste DATE,
    data_entrada_operacao DATE,
    data_desativacao DATE,
    potencia_efetiva FLOAT,
    combustivel VARCHAR(50),
    id_usina INTEGER REFERENCES Usina(id_usina)
);

-- 7. Tabela Acesso_Eletricidade
CREATE TABLE Acesso_Eletricidade (
    id SERIAL PRIMARY KEY,
    id_pais INTEGER REFERENCES Pais(id_pais),
    ano INTEGER NOT NULL,
    porcentagem FLOAT NOT NULL
);

-- 8. Tabela Acesso_Energia_Renovavel
CREATE TABLE Acesso_Energia_Renovavel (
    id SERIAL PRIMARY KEY,
    id_pais INTEGER REFERENCES Pais(id_pais),
    ano INTEGER NOT NULL,
    porcentagem FLOAT NOT NULL
);

-- 9. Tabela Acesso_Combustivel_Limpo
CREATE TABLE Acesso_Combustivel_Limpo (
    id SERIAL PRIMARY KEY,
    id_pais INTEGER REFERENCES Pais(id_pais),
    ano INTEGER NOT NULL,
    porcentagem FLOAT NOT NULL
);

-- 10. Tabela Investimento_Energia_Limpa    
CREATE TABLE Investimento_Energia_Limpa (
    id SERIAL PRIMARY KEY,
    id_pais INTEGER REFERENCES Pais(id_pais),
    ano INTEGER NOT NULL,
    valor_dolar FLOAT NOT NULL
);

-- 11. Tabela Energia_Renovavel_Per_Capita
CREATE TABLE Energia_Renovavel_Per_Capita (
    id SERIAL PRIMARY KEY,
    id_pais INTEGER REFERENCES Pais(id_pais),
    ano INTEGER NOT NULL,
    geracao_watts FLOAT NOT NULL
);

-- 12. Tabela IDH
CREATE TABLE IDH (
    id SERIAL PRIMARY KEY,
    id_pais INTEGER REFERENCES Pais(id_pais),
    ano INTEGER NOT NULL,
    indice FLOAT NOT NULL
);
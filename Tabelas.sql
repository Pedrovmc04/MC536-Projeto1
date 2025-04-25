-- 1. Tabela Estado (antiga Regiao)
CREATE TABLE Estado (
    id_estado SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    cod_estado VARCHAR(2) NOT NULL,
    administracao VARCHAR(100)
);

-- 6. Tabela Pais (moved up to be created before Regiao)
CREATE TABLE Pais (
    code VARCHAR(10) PRIMARY KEY,
    nome VARCHAR(100) NOT NULL
);

-- 2 Tabela Regiao (nova)
CREATE TABLE Regiao (
    id_regiao SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    id_pais VARCHAR(10) REFERENCES Pais(code)
);

-- 3. Tabela Agente_Proprietario
CREATE TABLE Agente_Proprietario (
    id_agente SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL
);

-- 4. Tabela Usina
CREATE TABLE Usina (
    id_usina SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    id_agente_proprietario INTEGER REFERENCES Agente_Proprietario(id_agente),
    data_entrada_teste DATE,
    data_entrada_operacao DATE,
    data_desativacao DATE,
    tipo VARCHAR(50),
    modalidade_operacao VARCHAR(50),
    id_estado INTEGER REFERENCES Estado(id_estado)
);

-- 5. Tabela Unidade_Geradora (MUDAR)
CREATE TABLE Unidade_Geradora (
    id_unidade SERIAL PRIMARY KEY,
    cod_equipamento VARCHAR(50) NOT NULL,
    nome_unidade VARCHAR(100),
    num_unidade INTEGER,
    potencia_efetiva FLOAT,
    combustivel VARCHAR(50),
    id_usina INTEGER REFERENCES Usina(id_usina)
);

-- 7. Tabela Acesso_Eletricidade (Mudar PK para ANOS)
CREATE TABLE Acesso_Eletricidade (
    id SERIAL PRIMARY KEY,
    code_pais VARCHAR(10) REFERENCES Pais(code),
    ano INTEGER NOT NULL,
    porcentagem FLOAT NOT NULL
);

-- 8. Tabela Acesso_Energia_Renovavel
CREATE TABLE Acesso_Energia_Renovavel (
    id SERIAL PRIMARY KEY,
    code_pais VARCHAR(10) REFERENCES Pais(code),
    ano INTEGER NOT NULL,
    porcentagem FLOAT NOT NULL
);

-- 9. Tabela Acesso_Combustivel_Limpo
CREATE TABLE Acesso_Combustivel_Limpo (
    id SERIAL PRIMARY KEY,
    code_pais VARCHAR(10) REFERENCES Pais(code),
    ano INTEGER NOT NULL,
    porcentagem FLOAT NOT NULL
);

-- 10. Tabela Investimento_Energia_Limpa
CREATE TABLE Investimento_Energia_Limpa (
    id SERIAL PRIMARY KEY,
    code_pais VARCHAR(10) REFERENCES Pais(code),
    ano INTEGER NOT NULL,
    valor_dolar FLOAT NOT NULL
);

-- 11. Tabela Energia_Renovavel_Per_Capita
CREATE TABLE Energia_Renovavel_Per_Capita (
    id SERIAL PRIMARY KEY,
    code_pais VARCHAR(10) REFERENCES Pais(code),
    ano INTEGER NOT NULL,
    geracao_watts FLOAT NOT NULL
);

-- 12. Tabela IDH
CREATE TABLE IDH (
    id SERIAL PRIMARY KEY,
    code_pais VARCHAR(10) REFERENCES Pais(code),
    ano INTEGER NOT NULL,
    indice FLOAT NOT NULL
);
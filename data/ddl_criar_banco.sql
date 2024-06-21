-- Tabela "pessoa"
CREATE TABLE pessoa (
    id_pessoa SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    cpf VARCHAR (14) NOT NULL,
    telefone VARCHAR(20),
    email VARCHAR(255),
    data_nasc DATE,
    rua VARCHAR(100),
    numero INT,
    bairro VARCHAR(50),
    cidade VARCHAR(50),
    estado VARCHAR(50),
    cep VARCHAR(10)
);

CREATE INDEX idx_pessoa_idade_estado ON pessoa(data_nasc, estado);

-- Tabela "seguradora"
CREATE TABLE seguradora (
    id_seguradora SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    cnpj CHAR(18) UNIQUE NOT NULL,
    razao_social varchar(100) NOT NULL,
    telefone VARCHAR(20),
    email VARCHAR(255),
    rua VARCHAR(100),
    numero INT,
    bairro VARCHAR(50),
    cidade VARCHAR(50),
    estado VARCHAR(50),
    cep VARCHAR(10)
);

-- indice para otimização de query
create index idx_estado_seguradora on seguradora (estado);

-- Tabela "Cliente"
CREATE TABLE cliente (
    id_cliente SERIAL PRIMARY KEY,
    id_pessoa INT NOT NULL,
    FOREIGN KEY (id_pessoa) REFERENCES pessoa(id_pessoa)
);

CREATE INDEX idx_fk_cliente_pessoa ON cliente (id_pessoa);

-- Tabela "Corretor"
CREATE TABLE corretor (
    id_seguradora INT NOT NULL,
    id_pessoa INT NOT NULL,
    FOREIGN KEY (id_pessoa) REFERENCES pessoa(id_pessoa),
    FOREIGN KEY (id_seguradora) REFERENCES seguradora(id_seguradora)
);

ALTER TABLE corretor ADD PRIMARY KEY (id_seguradora, id_pessoa);

-- Tabela "Imóvel"
CREATE TYPE public.tipo_imovel AS ENUM (
    'Casa',
    'Apartamento',
    'Terreno',
    'Sala Comercial',
    'Depósito'
);

CREATE TABLE imovel (
    id_imovel SERIAL PRIMARY KEY,
    tipo public.tipo_imovel NOT NULL,
    area DECIMAL(10,2) NOT NULL,
    valor DECIMAL(15,2) NOT NULL,
    descricao VARCHAR(255),
    id_seguradora INT NOT NULL,
    id_cliente INT NOT NULL,
    rua VARCHAR(100),
    numero INT,
    bairro VARCHAR(50),
    cidade VARCHAR(50),
    estado VARCHAR(50),
    cep VARCHAR(10),
    FOREIGN KEY (id_seguradora) REFERENCES seguradora(id_seguradora),
    FOREIGN KEY (id_cliente) REFERENCES cliente(id_cliente)
);

CREATE INDEX idx_fk_imovel_seguradora ON imovel(id_seguradora);
CREATE INDEX idx_fk_imovel_cliente ON imovel(id_cliente);

CREATE INDEX idx_tipo_valor_imovel ON imovel (id_imovel, tipo, valor);

-- Tabela "Mobília"
CREATE TABLE mobilia (
    id_mobilia SERIAL PRIMARY KEY,
    id_imovel INT NOT NULL,
    nome VARCHAR(255) NOT NULL,
    valor DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (id_imovel) REFERENCES imovel(id_imovel)
);

CREATE INDEX idx_fk_mobilia_imovel ON mobilia(id_imovel);

-- Tabela "Apólice"
CREATE TABLE apolice (
    id_apolice SERIAL PRIMARY KEY,
    id_imovel INT NOT NULL,
    id_corretor_pessoa INT NOT NULL,
    id_corretor_seguradora INT NOT NULL,
    data_inicio DATE NOT NULL,
    data_fim DATE NOT NULL,
    valor_cobertura DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (id_imovel) REFERENCES imovel(id_imovel),
    FOREIGN KEY (id_corretor_seguradora, id_corretor_pessoa) REFERENCES corretor(id_seguradora, id_pessoa)
);

CREATE INDEX idx_fk_apolice_imovel ON apolice(id_imovel);
CREATE INDEX idx_fk_apolice_corretor ON apolice(id_corretor_seguradora, id_corretor_pessoa);
-- indice para otimização de query
CREATE INDEX idx_fim_apolice ON apolice (data_fim);
CREATE INDEX idx_inicio_apolice ON apolice (data_inicio);

-- Tabela "Sinistro"
CREATE TABLE sinistro (
    id_sinistro SERIAL PRIMARY KEY,
    id_imovel INT NOT NULL,
    data_ocorrencia DATE NOT NULL,
    descricao TEXT NOT NULL,
    valor_prejuizo DECIMAL(15,2) NOT NULL,
    FOREIGN KEY (id_imovel) REFERENCES imovel(id_imovel)
);

CREATE INDEX idx_fk_sinistro_imovel ON sinistro (id_imovel);
CREATE INDEX idx_data_sinistro ON sinistro (data_ocorrencia);
-- Tabela "Vistoria"
CREATE TABLE Vistoria (
  id_vistoria SERIAL PRIMARY KEY,
  id_imovel INT NOT NULL,
  data_vistoria DATE NOT NULL,
  descricao TEXT NOT NULL,
  FOREIGN KEY (id_imovel) REFERENCES imovel(id_imovel)
);

CREATE INDEX idx_fk_vistoria_imovel ON vistoria (id_imovel);

-- Tabela "Histórico"
CREATE TABLE historico (
  id_historico SERIAL PRIMARY KEY,
  id_imovel INT NOT NULL,
  data_atualizacao DATE NOT NULL,
  descricao VARCHAR (500) NOT NULL
);



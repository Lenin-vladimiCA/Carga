-- ----------------------------
-- 1. TABLAS DE STAGING
-- ----------------------------

-- Staging_Customer
CREATE TABLE Staging_Customer (
    staging_customer_id SERIAL PRIMARY KEY,
    customerid_source INT NOT NULL,  -- ID original de la fuente
    customer_name VARCHAR(100),
    customer_city VARCHAR(100),
    load_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Staging_Product
CREATE TABLE Staging_Product (
    staging_product_id SERIAL PRIMARY KEY,
    productid_source INT NOT NULL,  -- ID original de la fuente
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    load_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Staging_OrderDetails
CREATE TABLE Staging_OrderDetails (
    staging_orderdetail_id SERIAL PRIMARY KEY,
    orderid_source INT NOT NULL,
    orderdate_source DATE NOT NULL,
    customerid_source INT NOT NULL,  -- FK al cliente en el sistema fuente
    productid_source INT NOT NULL,   -- FK al producto en el sistema fuente
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    load_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- ----------------------------
-- 2A. TABLAS DE DIMENSIÓN
-- ----------------------------

-- Dim_Customer
CREATE TABLE Dim_Customer (
    customer_key SERIAL PRIMARY KEY,  -- Clave subrogada (Surrogate Key)
    customerid_source INT NOT NULL UNIQUE, -- ID del sistema fuente
    customer_name VARCHAR(100) NOT NULL,
    customer_city VARCHAR(100),
    valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITHOUT TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE -- Para manejo de SCD Type 2
);

-- Dim_Product
CREATE TABLE Dim_Product (
    product_key SERIAL PRIMARY KEY,
    productid_source INT NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50),
    valid_from TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITHOUT TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dim_Date
-- La clave primaria es una clave natural (fecha), que también sirve como clave subrogada.
CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY, -- Formato YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week SMALLINT NOT NULL,
    day_name_es VARCHAR(15) NOT NULL,
    day_of_month SMALLINT NOT NULL,
    month_name_es VARCHAR(15) NOT NULL,
    month_number SMALLINT NOT NULL,
    calendar_quarter SMALLINT NOT NULL,
    calendar_year SMALLINT NOT NULL
);

-- ----------------------------
-- 2B. TABLA DE HECHOS
-- ----------------------------

CREATE TABLE Fact_OrderDetails (
    order_detail_key BIGSERIAL PRIMARY KEY,
    
    -- Claves foráneas a las dimensiones
    date_key INT NOT NULL, 
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    
    -- Hechos/Métricas
    orderid_source INT NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    sales_amount NUMERIC(10, 2) NOT NULL, -- Cálculo: Quantity * Unit_Price
    
    -- Definición de Claves Foráneas
    CONSTRAINT fk_date
        FOREIGN KEY (date_key)
        REFERENCES Dim_Date (date_key),
    
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_key)
        REFERENCES Dim_Customer (customer_key),
        
    CONSTRAINT fk_product
        FOREIGN KEY (product_key)
        REFERENCES Dim_Product (product_key)
);
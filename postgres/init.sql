-- /Users/clifflolo/Desktop/Hubtel/nyt-etl/postgres/init.sql

-- Switch to the nyt_db database
\c nyt_db;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_book_rankings CASCADE;
DROP TABLE IF EXISTS fact_publisher_performance CASCADE;
DROP TABLE IF EXISTS dim_book CASCADE;
DROP TABLE IF EXISTS dim_list CASCADE;
DROP TABLE IF EXISTS dim_publisher CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS load_status CASCADE;

-- 1. Dimension Table: dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    quarter_name VARCHAR(10),
    month INT NOT NULL,
    month_name VARCHAR(20),
    week_of_year INT,
    week_start_date DATE,
    week_end_date DATE,
    bestsellers_date DATE,
    published_date DATE,
    previous_published_date DATE,
    next_published_date DATE
);

-- 2. Dimension Table: dim_publisher
CREATE TABLE IF NOT EXISTS dim_publisher (
    publisher_key SERIAL PRIMARY KEY,
    publisher_name VARCHAR(255) NOT NULL,
    effective_start_date DATE DEFAULT CURRENT_DATE,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    CONSTRAINT uk_publisher_name_dates UNIQUE (publisher_name, effective_start_date)
);

-- 3. Dimension Table: dim_list
CREATE TABLE IF NOT EXISTS dim_list (
    list_key SERIAL PRIMARY KEY,
    list_id INT NOT NULL,
    list_name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    update_frequency VARCHAR(50),
    list_image_url VARCHAR(500),
    effective_start_date DATE DEFAULT CURRENT_DATE,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    CONSTRAINT uk_list_id_dates UNIQUE (list_id, effective_start_date)
);

-- 4. Dimension Table: dim_book
CREATE TABLE IF NOT EXISTS dim_book (
    book_key SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author VARCHAR(255),
    contributor VARCHAR(255),
    contributor_note TEXT,
    age_group VARCHAR(50),
    publisher_key INT REFERENCES dim_publisher(publisher_key),
    primary_isbn13 VARCHAR(13),
    primary_isbn10 VARCHAR(10),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP,
    effective_start_date DATE DEFAULT CURRENT_DATE,
    effective_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    CONSTRAINT uk_isbn13_dates UNIQUE (primary_isbn13, effective_start_date)
);

-- 5. Fact Table: fact_book_rankings
CREATE TABLE IF NOT EXISTS fact_book_rankings (
    ranking_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    book_key INT REFERENCES dim_book(book_key),
    list_key INT REFERENCES dim_list(list_key),
    rank INT NOT NULL,
    price DECIMAL(10,2),
    CONSTRAINT uk_book_list_date UNIQUE (date_key, book_key, list_key)
);

-- 6. Fact Table: fact_publisher_performance
CREATE TABLE IF NOT EXISTS fact_publisher_performance (
    performance_key SERIAL PRIMARY KEY,
    publisher_key INT REFERENCES dim_publisher(publisher_key),
    date_key INT REFERENCES dim_date(date_key),
    list_key INT REFERENCES dim_list(list_key),
    quarter INT,
    year INT,
    total_points INT,
    books_in_top_5 INT,
    rank_1_count INT,
    rank_2_count INT,
    rank_3_count INT,
    rank_4_count INT,
    rank_5_count INT,
    CONSTRAINT uk_publisher_date_list UNIQUE (publisher_key, date_key, list_key)
);

-- 7. Load Status Table
CREATE TABLE IF NOT EXISTS load_status (
    id SERIAL PRIMARY KEY,
    requested_date DATE NOT NULL,
    bestsellers_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    error_message TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (requested_date, bestsellers_date)
);


CREATE USER marquez WITH PASSWORD 'marquez';
CREATE DATABASE marquez OWNER marquez;
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;

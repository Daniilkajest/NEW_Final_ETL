CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS marts;

-- 1. Создаем таблицу сессий с партиционированием по времени начала сессии (start_time)
-- В PostgreSQL при партиционировании ключ партиции должен входить в Primary Key
CREATE TABLE IF NOT EXISTS ods.user_sessions (
    session_id VARCHAR(100),
    user_id VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device VARCHAR(50),
    actions TEXT[],
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

-- Создаем дефолтную партицию и партиции по месяцам для сессий
CREATE TABLE ods.user_sessions_default PARTITION OF ods.user_sessions DEFAULT;

CREATE TABLE ods.user_sessions_2024_01 PARTITION OF ods.user_sessions 
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE ods.user_sessions_2024_02 PARTITION OF ods.user_sessions 
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- 2. Создаем таблицу логов поддержки
CREATE TABLE IF NOT EXISTS ods.support_tickets (
    ticket_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(100),
    status VARCHAR(50),
    issue_type VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
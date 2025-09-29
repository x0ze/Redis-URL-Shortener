CREATE DATABASE IF NOT EXISTS urldb;
USE urldb;
CREATE TABLE IF NOT EXISTS url_table (
    originalUrl varchar(255),
    shortUrl varchar(255),
    clickCounter INT DEFAULT 0,
    expireDate DATETIME
);
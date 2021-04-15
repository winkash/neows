CREATE DATABASE if not exists neows_db;
use neows_db;
CREATE TABLE IF NOT EXISTS neows (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    neo_reference_id INT NOT NULL,
    is_potentially_hazardous_asteroid BOOLEAN NOT NULL DEFAULT FALSE,
    miss_distance DOUBLE(40,10),
    close_approach_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)  ENGINE=INNODB;

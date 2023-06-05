CREATE TABLE high_load_prototyping (
    user_id INT,
    accno VARCHAR(255),
    user_sid_complete VARCHAR(255),
    porto_date DATE,
    porto_stock_code VARCHAR(255),
    porto_stock_quantity FLOAT,
    porto_last_price FLOAT,
    porto_avg_price FLOAT,
    porto_amount FLOAT,
    has_credit BOOLEAN
);

CREATE INDEX cidx_stock_code_has_credit ON high_load_prototyping (porto_stock_code, has_credit);

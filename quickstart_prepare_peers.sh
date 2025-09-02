#!/usr/bin/env bash
set -xeuo pipefail

# This script creates databases on the PeerDB internal cluster to be used as peers later.

CONNECTION_STRING="${1:-postgres://postgres:postgres@localhost:9901/postgres}"

if ! type psql >/dev/null 2>&1; then
  echo "psql not found on PATH, exiting"
  exit 1
fi

psql "$CONNECTION_STRING" << EOF

--- Create the databases
DROP DATABASE IF EXISTS source;
CREATE DATABASE source;
DROP DATABASE IF EXISTS target;
CREATE DATABASE target;

--- Switch to source database
\c source

--- Create customers table
CREATE TABLE customers (
    id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

--- Create products table
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    weight NUMERIC(10, 2)
);

--- Create orders table
CREATE TABLE orders (
    id INT PRIMARY KEY,
    order_date DATE,
    purchaser INT,
    quantity INT,
    product_id INT,
    FOREIGN KEY (purchaser) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);



--- Insert sample data
INSERT INTO customers (id, first_name, last_name, email) VALUES
(1001, 'Sally', 'Thomas', 'sally.thomas@acme.com'),
(1002, 'George', 'Bailey', 'gbailey@foobar.com'),
(1003, 'Edward', 'Walker', 'ed@walker.com'),
(1004, 'Anne', 'Kretchmar', 'annek@noanswer.org');

INSERT INTO products (id, name, description, weight) VALUES
(101, 'scooter', 'Small 2-wheel scooter', 3.14),
(102, 'car battery', '12V car battery', 8.10),
(103, '12-pack drill bits', '12-pack of drill bits with sizes ranging from #40 to #3', 0.80),
(104, 'hammer', '12oz carpenter''s hammer', 0.75),
(105, 'hammer', '14oz carpenter''s hammer', 0.875),
(106, 'hammer', '16oz carpenter''s hammer', 1.00),
(107, 'rocks', 'box of assorted rocks', 5.30),
(108, 'jacket', 'water resistent black wind breaker', 0.10),
(109, 'spare tire', '24 inch sparetire',22.20);


INSERT INTO orders (id, order_date, purchaser, quantity, product_id) VALUES
(10001, '2016-01-16', 1001, 1, 102),
(10002, '2016-01-17', 1002, 2, 105),
(10003, '2016-02-19', 1002, 2, 106),
(10004, '2016-02-21', 1003, 1, 107),
(10005, '2025-07-25',1001,99,101);

-- Switch to target database
\c target

EOF
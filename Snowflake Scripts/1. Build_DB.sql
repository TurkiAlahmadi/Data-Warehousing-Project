-- Create and use database
CREATE DATABASE IF NOT EXISTS TPCDS;
USE TPCDS;

-- Create a schema for raw data
CREATE SCHEMA IF NOT EXISTS RAW;

-- Create a schema for staging data
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;

-- Create inventory table
CREATE OR REPLACE TABLE RAW.INVENTORY (
    inv_date int NOT NULL, 
    inv_item int NOT NULL, 
    inv_quantity_on_hand int, 
    inv_warehouse_sk int NOT NULL
);

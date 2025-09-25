CREATE TABLE gold.transactions_normal (
	id varchar(255) NULL,
	account_num varchar(255) NULL,
	trx_date date NULL,
	total_trx int4 NULL,
	total_amount numeric(15, 2) NULL,
	total_debit numeric(15, 2) NULL,
	total_credit numeric(15, 2) NULL,
	failed_trx int4 NULL,
	anomaly_trx int4 NULL,
	created_by varchar(255) NULL,
	created_date timestamp DEFAULT now() NULL,
	updated_by varchar(255) NULL,
	updated_date timestamp DEFAULT now() NULL
);
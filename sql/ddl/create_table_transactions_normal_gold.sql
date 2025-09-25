CREATE TABLE gold.transactions_normal (
	id uuid NOT NULL,
	account_num varchar(50) NOT NULL,
	trx_date date NOT NULL,
	total_trx int4 NOT NULL,
	total_amount numeric(18, 2) NOT NULL,
	total_debit numeric(18, 2) NOT NULL,
	total_credit numeric(18, 2) NOT NULL,
	failed_trx int4 NOT NULL,
	anomaly_trx int4 NOT NULL,
	created_by varchar(50) NULL,
	created_date timestamp NULL,
	updated_by varchar(50) NULL,
	updated_date timestamp NULL,
	CONSTRAINT transactions_normal_pkey PRIMARY KEY (id)
);

CREATE TABLE bronze.transactions_raw (
	id varchar(50) NOT NULL,
	trx_type varchar(50) NULL,
	account_number varchar(20) NULL,
	amount int8 NULL,
	debit_credit varchar(1) NULL,
	subheader varchar(255) NULL,
	detail_information varchar(50) NULL,
	trx_date date NULL,
	trx_time time NULL,
	currency varchar(3) NULL,
	created_date timestamp NULL,
	created_by varchar(50) NULL,
	updated_date timestamp NULL,
	updated_by varchar(50) NULL,
	CONSTRAINT transactions_raw_pkey PRIMARY KEY (id)
);
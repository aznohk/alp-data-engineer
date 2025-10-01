CREATE TABLE bronze.data_nasabah_raw (
	name varchar(255) NULL,
	account_number varchar(20) NOT NULL,
	phone_number varchar(20) NULL,
	address text NULL,
	status varchar(20) NULL,
	current_balance int8 NULL,
	created_by varchar(50) NULL,
	created_date timestamp NULL,
	updated_by varchar(50) NULL,
	updated_date timestamp NULL,
	CONSTRAINT data_nasabah_raw_pkey PRIMARY KEY (account_number)
);
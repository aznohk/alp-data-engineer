CREATE TABLE gold.transactions_summary (
	trx_date date NULL,
	tipe_anomali varchar(50) NULL,
	level_anomali varchar(20) NULL,
	transaksi_success int4 NULL,
	transaksi_failed int4 NULL,
	total_amount_transaksi numeric(15, 2) NULL,
	total_transaksi int4 NULL,
	currency varchar(10) NULL,
	created_by varchar(255) NULL,
	created_date timestamp DEFAULT now() NULL
);
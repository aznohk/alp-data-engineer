CREATE TABLE gold.transactions_summary (
	trx_date date NOT NULL,
	tipe_anomali varchar(20) NOT NULL,
	level_anomali varchar(20) DEFAULT 'Low'::character varying NULL,
	transaksi_success int4 NOT NULL,
	transaksi_failed int4 NOT NULL,
	total_amount_transaksi numeric(18, 2) NOT NULL,
	total_transaksi int4 NOT NULL,
	currency varchar(5) DEFAULT 'IDR'::character varying NULL,
	created_by varchar(50) DEFAULT 'Python Script'::character varying NULL,
	created_date timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT transactions_summary_pkey PRIMARY KEY (trx_date, tipe_anomali)
);

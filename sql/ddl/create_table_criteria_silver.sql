CREATE TABLE silver.criteria (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	code varchar(1) NOT NULL,
	description varchar(255) NULL,
	"level" varchar(30) NOT NULL,
	createdby varchar(30) DEFAULT 'SYSTEM'::character varying NOT NULL,
	createddate timestamptz DEFAULT now() NULL,
	updatedby varchar(30) DEFAULT 'SYSTEM'::character varying NOT NULL,
	updateddate timestamptz DEFAULT now() NULL,
	CONSTRAINT criteria_pkey PRIMARY KEY (id)
);
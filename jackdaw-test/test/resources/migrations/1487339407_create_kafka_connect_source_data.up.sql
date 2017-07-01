CREATE TABLE kafka_connect_source_data (
    id SERIAL,
    foreign_id uuid NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);

ALTER TABLE ONLY kafka_connect_source_data
    ADD CONSTRAINT kafka_connect_source_data_pkey PRIMARY KEY (id);

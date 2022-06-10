-- Create message

CREATE TABLE public.message
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 ),
    content character varying,
    PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS public.message
    OWNER to admin;

-- Create message_type

CREATE TABLE public.message_type
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 ),
    message_id bigint NOT NULL,
    type character varying,
    PRIMARY KEY (id),
    CONSTRAINT message_id_fk1 FOREIGN KEY (message_id)
        REFERENCES public.message (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

ALTER TABLE IF EXISTS public.message_type
    OWNER to admin;

-- Create message_type partition

CREATE TABLE message_type_partitioned (LIKE message_type INCLUDING ALL EXCLUDING CONSTRAINTS EXCLUDING INDEXES, PRIMARY KEY (id, message_id)) PARTITION BY RANGE(message_id);

CREATE TABLE message_type_20220001 PARTITION OF message_type_partitioned FOR VALUES FROM (1) TO (3);

-- Create trigger insert

CREATE OR REPLACE FUNCTION message_type_partitioned_insert() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO message_type_partitioned(message_id, type) VALUES (NEW.message_id, NEW.type);
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER copy_message_type_trigger AFTER INSERT ON message_type FOR EACH ROW EXECUTE FUNCTION message_type_partitioned_insert();


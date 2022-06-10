CREATE OR REPLACE FUNCTION message_type_partitioned_new_partition_creator() RETURNS TRIGGER AS $$
DECLARE
    next_message_id bigint;
    next_partition_name character varying;
BEGIN
    SELECT currval('message_id_seq') INTO next_message_id;
    raise notice 'next_message_id: %', next_message_id;
    if MOD(next_message_id, 3) = 0 then
        SELECT concat('message_', next_message_id) INTO next_partition_name;
        raise notice 'next_partition_name: %', next_partition_name;
        CREATE TABLE next_partition_name PARTITION OF message_type_partitioned FOR VALUES FROM (next_message_id) TO (next_message_id+3);
    END if;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE TRIGGER create_new_partition_message_type_trigger BEFORE INSERT ON message FOR EACH ROW EXECUTE FUNCTION message_type_partitioned_new_partition_creator();

INSERT INTO public.message(content) VALUES ('content one');
INSERT INTO public.message(content) VALUES ('content two');

INSERT INTO public.message_type(message_id,type) VALUES (1, 'a');
INSERT INTO public.message_type(message_id,type) VALUES (1, 'b');
INSERT INTO public.message_type(message_id,type) VALUES (2, 'a');

-- this execution should trigger partition creation

INSERT INTO public.message(content) VALUES ('content 3');

INSERT INTO public.message_type(message_id,type) VALUES (3, 'a');

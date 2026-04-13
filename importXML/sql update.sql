SELECT *
FROM credenciada
WHERE email IS NULL
   OR email = '';

update credenciada set email = 'teste@teste.com'
WHERE email IS NULL
   OR email = '';

   commit;

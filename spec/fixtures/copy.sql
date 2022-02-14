INSERT INTO %{shadow_table} ("username", "seller_id", "password", "email", "createdOn", "last_login", "user_id")
SELECT "username", "seller_id", "password", "email", "createdOn", "last_login", "user_id"
FROM ONLY books

CREATE TABLE contacts ( contact_id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, phone TEXT NOT NULL UNIQUE );

CREATE TABLE idontknow (id INTEGER);
INSERT INTO idontknow (id) VALUES (0);
SELECT * FROM idontknow;
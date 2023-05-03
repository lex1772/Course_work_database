CREATE TABLE vacancies
(
	vacancy_id serial PRIMARY KEY,
	employer varchar NOT NULL,
	vacancy_name varchar,
	salary int,
	url varchar NOT NULL,
	experience varchar NOT NULL,
	metro varchar,
	address varchar
)
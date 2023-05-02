CREATE TABLE vacancies
(
	vacancy_id int PRIMARY KEY,
	employer varchar(50) NOT NULL,
	vacancy_name varchar(50),
	salary_from int,
	salary_to int,
	url varchar(50) NOT NULL,
	experience varchar(50) NOT NULL,
	metro varchar(50),
	address varchar(50),
	description varchar
);
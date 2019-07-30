

create table titanic (
	Id INT not null,
	Survived boolean,
	Class INT,
	Name varchar(255),
	Sex varchar(8),
	Age float,
	SibSp INT,
	Parch INT,
	Ticket varchar(24),
	Fare float,
	Cabin varchar(24),
	Embarked varchar(24)
);

-- Importing the csv file data in the tables creates
LOAD DATA LOCAL INFILE  '/var/lib/mysql-files/titanic.csv' into table titanic
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r'
IGNORE 1 LINES;

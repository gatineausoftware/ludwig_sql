

CREATE TABLE agent (
  agent_id INTEGER,
  effective_date DATE,
  feature1 INTEGER,
  feature2 INTEGER,
  CONSTRAINT agent_PK PRIMARY KEY (agent_id, effective_date)
);

insert into agent
VALUES

(201, '2018-01-01', 4, 44),
(201, '2019-01-01', 6, 44),
(202, '2018-01-01', 14, 54),
(202, '2019-01-01', 16, 54);


CREATE TABLE complaints_history (
	complaint_id INTEGER,
  agent_id INTEGER,
	complaint_date DATE,
	complaint_feature INTEGER,
	CONSTRAINT complaints_history_PK PRIMARY KEY (complaint_id)
);

insert into complaints_history
VALUES
(101, 201, '2018-06-01', 5),
(102, 201, '2019-06-01', 6),
(103, 201, '2019-06-01', 7),
(104, 202, '2018-06-01', 3),
(105, 202, '2019-06-02', 4),
(106, 202, '2018-01-01', 44);


CREATE TABLE agent_eol (
  agent_id INTEGER,
  obvservation_date DATE,
  label INTEGER,
  CONSTRAINT agent_eol_PK PRIMARY KEY (agent_id, obvservation_date)
);

insert into agent_eol
VALUES
(201, '2018-06-02', 0),
(201, '2019-05-02', 1),
(201, '2019-06-01', 0),
(202, '2018-06-01', 0),
(202, '2019-06-02', 0);



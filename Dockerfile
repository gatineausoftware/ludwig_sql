FROM mysql:5.6
ENV MYSQL_DATABASE ml
COPY ./sql-scripts/ /docker-entrypoint-initdb.d/

build mysql image:

docker build -t mysql_ml .


run image:

docker run --name mysql -v /Users/benmackenzie/projects/mysql:/var/lib/mysql-files/ -e MYSQL_ROOT_PASSWORD=swamp -p 3306:3306 mysql_ml



so far:

- load titanic file into mysql
- read into a dataframe
- define ludwig Model
- train with data frame


to do:

- build model definition automatically
- provide visual mechanism to select target
- automatically load into fastscore  [generate schema, generate model conformance python, generate tar file, use fastscore deploy api to add model]

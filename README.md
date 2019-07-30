build mysql image:

docker build -t mysql_ml .


run image:

docker run --name mysql -v /Users/benmackenzie/projects/mysql:/var/lib/mysql-files/ -e MYSQL_ROOT_PASSWORD=swamp mysql_ml

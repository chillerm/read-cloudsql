FROM mysql:5.7

ENV MYSQL_DATABASE=test
ENV MYSQL_ROOT_PASSWORD=test
ENV MYSQL_USER=test
ENV MYSQL_PASSWORD=test

COPY test_data.sql /docker-entrypoint-initdb.d/

COPY my.cnf /etc/mysql/my.cnf

RUN /entrypoint.sh mysqld & sleep 10 && killall mysqld

RUN rm /docker-entrypoint-initdb.d/test_data.sql

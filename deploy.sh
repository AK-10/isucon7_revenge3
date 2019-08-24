#!/bin/bash

if [ $# == 1 ]; then
	echo "execute checkout and pull!!"
	git reset --hard HEAD && git fetch && git checkout $1 && git pull origin $1
fi

## Database data reset
#sudo ./db/init.sh
#zcat ~/isubata/bench/isucon7q-initial-dataset.sql.gz | sudo mysql isubata

## application build
(
cd webapp/go
make
)

## replaces log data 
LOGPATH=/var/log
NOW=`date +'%H-%M-%S'`

sudo cp $LOGPATH/nginx/access.log $LOGPATH/nginx/access-$NOW.log
sudo sh -c 'echo "" > /var/log/nginx/access.log'

sudo cp $LOGPATH/mysql/slow.log $LOGPATH/mysql/slow-$NOW.log
sudo sh -c 'echo "" > /var/log/mysql/slow.log'

## replace mysql conf
sudo cp conf/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf

## restart application services
## db, app, nginx, redis
echo 'systemctl are restarting...'
sudo systemctl restart mysql.service
sudo systemctl restart isubata.golang.service
sudo systemctl restart nginx.service
#sudo systemctl restart redis.service
echo 'Finished to restart!!'

## execute bench marker and analysis tools
(
cd bench
./bin/bench -remotes=127.0.0.1 -output result.json
)
jq . < bench/result.json
sudo /usr/local/bin/alp -f /var/log/nginx/access.log -r --sum | head -n 30
sudo mysqldumpslow -s t /var/log/mysql/slow.log | head -n 30


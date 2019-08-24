#!/bin/sh

# basic execution
HOSTS="isucon7revenge3"
echo "Deploy Start"
for host in ${HOSTS}; do
    echo "CURRENT_HOST: $host"
    # 極力フルパスじゃないと通らない
	ssh -t $host <<EOC
    cd isubata
    if [ $# == 1 ]; then
        echo "execute checkout and pull!!"
        git reset --hard HEAD && git fetch && git checkout $1 && git pull origin $1
    fi

    ## Database data reset
    #sudo ./db/init.sh
    #zcat ~/isubata/bench/isucon7q-initial-dataset.sql.gz | sudo mysql isubata

    ## application build
    export PATH="$PATH:/home/isucon/local/go/bin"
    printenv
    (
    cd webapp/go
    make
    )

    # replaces log data 
    sudo sh -c 'echo "" > /var/log/nginx/access.log'
    sudo sh -c 'echo "" > /var/log/mysql/slow.log'

    # replace mysql conf
    ls
    sudo cp conf/mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf

    # replace redis conf
    #sudo cp conf/redis.conf /etc/redis/redis.conf
    #sudo chown redis:redis /etc/redis/redis.conf

    # restart application services
    # db, app, nginx, redis
    echo 'systemctl are restarting...'
    sudo systemctl restart mysql.service
    sudo systemctl restart isubata.golang.service
    sudo systemctl restart nginx.service
    #sudo systemctl restart redis.service
    echo 'Finished to restart!!'

    # execute bench marker and analysis tools
    (
    cd bench
    ./bin/bench -remotes=127.0.0.1 -output result.json
    )
    jq . < bench/result.json
    sudo /usr/local/bin/alp -f /var/log/nginx/access.log -r --sum | head -n 30
    sudo mysqldumpslow -s t /var/log/mysql/slow.log | head -n 30
    echo "CURRENT HOST ENDED: $host"
EOC
done

# special execution
#ssh -t isucon81 <<EOC
#	# do something
#EOC
#ssh -t isucon82 <<EOC
#	# do something
#EOC
#ssh -t isucon83 <<EOC
#	# do something
#EOC
echo "Deploy Ended"


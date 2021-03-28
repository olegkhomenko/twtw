# twtw
Analytics for twitter profiles 

### Requirements:
`RabbitMQ, Celery, MongoDB`

#### Install MongoDB
```bash
brew tap mongodb/brew
brew install mongodb-community@4.4

# run
brew services start mongodb/brew/mongodb-community
# or
mongod --config /usr/local/etc/mongod.conf
```

#### Install RabbitMQ
```bash
brew install rabbitmq
```

#### Run RabbitMQ
```bash
rabbitmq-server
```
or
```bash
brew services start rabbitmq
```

## Run
```
celery -A celery_app worker --loglevel=info
```

## Tests
```basg
python3 -m unittest discover twtw/tests
```
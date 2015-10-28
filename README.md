# ROMA - A Distributed Key-Value Store in Ruby

ROMA is one of the data storing systems for distributed key-value stores.  
It is a completely decentralized distributed system that consists of multiple
processes, called nodes, on several machines. It is based on pure P2P architecture  
like a distributed hash table, thus it provides high availability and scalability.

ROMA is written in Ruby. However, following choices are available to
access to ROMA.

* Client libraries of Ruby and Java are available.
* ROMA protocol is compatible with memcached text-based one so that  
  any memcached client libraries allows users to interact with ROMA.

More information is [here](http://roma-kvs.org/ "ROMA").

## Documentation

* Refer to [ROMA documentations](http://roma-kvs.org/ "ROMA")

## Requirements
* Ruby >= 2.1.0



## Installation
### Install ROMA
You can simply install ROMA and dependency libralies by using a "gems" command of Ruby as follows.  

```
$ gem install roma
```

### Make routing files

ROMA is required to make the routing files before starting up.  
The routing file is stored the routing information of each processes.

```
$ mkroute localhost_10001 localhost_10002 --replication_in_host
```

If succeeded, two new files which named localhost_10001.route and localhost_10002.route created in the current directory.  
Refer to [Commands](http://roma-kvs.org/commands.html "Commands") for more detail information about Shell Commands.  

### Start up ROMA
Run two processes by using a romad.rb program as follows:  

```
$ romad localhost -p 10001 -d --replication_in_host
$ romad localhost -p 10002 -d --replication_in_host
```

Refer to [Commands](http://roma-kvs.org/commands.html "Commands") for more detail information about Shell Commands.  

## Usage
Like memcached, you can connect to ROMA with telnet. Connect to the ROMA process that you ran above.

```
$ telnet localhost 10001
```

You can interact with ROMA in the same way of memcached commands.

```
set foo 0 0 3 <return>
bar <return>
STORED
get foo <return>
VALUE foo 0 3
bar
END
```

Refer to [Commands](http://roma-kvs.org/commands.html "Commands") for more detail information about ROMA Commands.


## Contributing

If you would like to contribute, please...

1. Fork.
2. Download [Ruby Client](https://github.com/roma/roma-ruby-client) to same directory.
3. Make changes in a branch & add unit tests.
4. Run Unit Test
  * `ruby test/run_test.rb` (if unit test fails, run it again - it's fickle).
5. Create a pull request.

Contributions, improvements, comments and suggestions are welcome!

## Promoters
Roma is promoted by [Rakuten, Inc.](http://global.rakuten.com/corp/) and [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).

# ROMA - A Distributed Key-Value Store in Ruby 
[![Gitter](https://badges.gitter.im/roma/roma.svg)](https://gitter.im/roma/roma?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/roma/roma.svg?branch=master)](https://travis-ci.org/roma/roma)
[![Test Coverage](https://codeclimate.com/github/roma/roma/badges/coverage.svg)](https://codeclimate.com/github/roma/roma/coverage)
[![Code Climate](https://codeclimate.com/github/roma/roma/badges/gpa.svg)](https://codeclimate.com/github/roma/roma)
[![Issue Count](https://codeclimate.com/github/roma/roma/badges/issue_count.svg)](https://codeclimate.com/github/roma/roma)

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
You can simply install ROMA and dependency libralies by using a `gem` command of Ruby as follows.

```
$ gem install roma
```

### Troubleshooting

#### Building failure about gem native extension (Ubuntu 14.04)

On Ubuntu 14.04, it reports a failure about building gem native extension while doing `bundle install` under ROMA source directory:

    Using tokyocabinet 1.32.0 from git://github.com/roma/tokyocabinet-ruby.git (at master@f270943)

    Gem::Ext::BuildError: ERROR: Failed to build gem native extension.

This is because of the lack of `libbz2-dev`, so that the `tokyocabinet` cannot be built.
However, the error message is not very helpful, unless one takes a look at the `mkmf.log`.

    /usr/bin/ld: cannot find -lbz2

To solve it, install the package from `apt-get`:

    sudo apt-get install libbz2-dev

Then re-run `bundle install` to install all the depencies.

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

Refer to [Shell Commands](http://roma-kvs.org/commands.html#dist-jump-link-shell_commands) for more detail information about Shell Commands.  

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

1. Fork and git clone it.
2. Install gems required for development.
3. Make changes in a branch & add unit tests.
4. Run Unit Test
  * `bundle exec rake` (if unit test fails, run it again - it's fickle).
  * Specify `STORAGE` to test test cases related to storages such as groonga, sqlite3 and dbm.
5. Create a pull request.

Contributions, improvements, comments and suggestions are welcome!

## Promoters
Roma is promoted by [Rakuten, Inc.](http://global.rakuten.com/corp/) and [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).

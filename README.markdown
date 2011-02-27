cbloader
=======

Yet another load generating tool for use while testing the behaviour and performance of memcached. Currently very basic

Dependencies
============

Erlang R13B04 or greater

Building
========

        git clone git://github.com/daleharvey/cbloader.git
        cd cbloader
        make

or download [https://github.com/daleharvey/cbloader/raw/master/cbloader](https://github.com/daleharvey/cbloader/raw/master/cbloader) and place it in your $PATH

Usage
=====

    $ cbloader
    $ cbloader -n 10k -v 1kb -p 5 -s 127.0.0.1:12001
    $ cbloader --num-keys 10k --processes 5 --payload-size 1kb --servers 127.0.0.1:12001

### Arguments

        -n / --num_keys X
    The number of keys to write to memcached eg: 1, 100, 50k, 3m

        -v / --value-size X
    The size of values to write eg: 500, 5kb, 3mb

        -s / --servers [hostname:memcachedport]
    The servers to write to eg: 127.0.0.1:12001,127.0.0.1:12003

        -p / --processes X
    The number of processes that write to each server, must be larger than and
    a divisor of num_keys

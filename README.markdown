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
    $ cbloader -n 10k -p 1kb -s 127.0.0.1:12001 -kg rand
    $ cbloader --num-keys 10k --payload-size 1kb --servers 127.0.0.1:120001 --key-gen rand

### Arguments

        -n / --num_keys X
    The number of keys to write to memcached eg: 1, 100, 50k, 3m

        -p / --payload-size X
    The size of values to write eg: 500, 5kb, 3mb

        -s / --servers [hostname:memcachedport]
    The servers to write to eg: 127.0.0.1:12001,127.0.0.1:12003

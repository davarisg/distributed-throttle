# Distributed Throttle
--------------

## Summary
The Perl `Throttle` class implements a distributed throttler for your application. It uses a Redis database to share the RPM and all the necessary information for throttling a single resource used from multiple sources. All the operations are guaranteed to be atomic. To use the shared resource each endpoint must acquire a mutex also implemented through Redis.

## Requirements
Packages Required:

* redis-server
* perl
* liblog-log4perl-perl
* libredis-perl

## Usage
To use the module follow the steps:

    user@debian:~ $ git clone https://github.com/davarisg/distributed-throttle.git /srv/distributed-throttle
    Cloning into 'distributed-throttle'...
    remote: Counting objects: 26, done.
    remote: Compressing objects: 100% (17/17), done.
    remote: Total 26 (delta 7), reused 22 (delta 6)
    Unpacking objects: 100% (26/26), done.

This is a sample perl script that uses the `Throttle` module to rate limit its iterations:

    user@debian:~ $ cat throttle_application.pl

    use strict;
    use warnings;

    use lib '/srv/distributed-throttle/lib';

    use Throttle ();

    my $t = Throttle->new(
        'rpm'   => 240,
    );

    foreach (1..60) {
        $t->throttle();
    }

    user@debian:~ $ time perl throttle_application.pl
    Log4perl: Seems like no initialization happened. Forgot to call init()?

    real    0m15.115s
    user    0m0.064s
    sys     0m0.168s

These are all the available parameters for the `new` method:

    $t = Throttle->new(
        'count_key'         => 't:mycustomcounter',
        'redis_db'          => 1,
        'reset_threshold'   => 15000000,
        'rpm'               => 60,
        'timer_key'         => 't:mycustomtimer',
    );

* 'count\_key' => Redis key that holds total count of operations completed (Defaults to 't:count')
* 'redis\_db' => Redis DB where keys will be stored (Defaults to 0)
* 'reset\_threshold' => Counter will reset to 0 after it reaches the threshold (Defaults to 1000000)
* 'rpm' => Requests per minute
* 'timer\_key' => Redis key that hold the value of the timer (Defaults to 't:timer')

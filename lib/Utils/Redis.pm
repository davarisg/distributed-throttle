package Utils::Redis;

use strict;
use warnings;

use Log::Log4perl ();
use Redis ();

# Utils::Redis - Redis wrapper module
#
# Author - Georgios Davaris
#
# This module acts like a wrapper to Redis.pm.
# It is a dependency to Throttle.pm and Utils::Semaphore.
#
# The module implements the Redis blpop, del, get, getset,
# incr, lpush and set commands.

my $conn;

sub connection() {
    return $conn if ($conn);

    $conn = Redis->new(
        'reconnect'     => 60,
    );

    return $conn;
}

sub blpop($$;$) {
    my ($db, $key, $timeout) = @_;
    $timeout ||= 0;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless($key);

    $conn->select($db);

    $logger->trace("Blocking list call for key '$key'");
    my $value = $conn->blpop($key, $timeout);
    $logger->trace("BLPOP returned:");
    $logger->trace({
        'filter'        => \&Data::Dumper::Dumper,
        'value'         => $value,
    });

    return $value;
}

sub del($@) {
    my ($db, @keys) = @_;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless(scalar(@keys));

    $conn->select($db);

    $logger->trace("Deleting keys");
    $logger->trace({
        'filter'        => \&Data::Dumper::Dumper,
        'value'         => \@keys
    });

    foreach (@keys) {
        $conn->del($_);
    }

    return;
}

sub get($$) {
    my ($db, $key) = @_;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless($key);

    $conn->select($db);

    $logger->trace("GET key '$key'");
    my $value = $conn->get($key);
    $logger->trace("Get value: $value") if ($value);

    return $value;
}

sub getset($$$) {
    my ($db, $key, $value) = @_;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless($key && $value);

    $conn->select($db);

    $logger->trace("GETSET on key '$key'");
    my $old_value = $conn->getset($key, $value);

    if ($old_value) {
        $logger->trace("GetSet value: $old_value");
    }
    else {
        $logger->trace("Key was undefined");
    }

    return $old_value;
}

sub incr($$) {
    my ($db, $key) = @_;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless($key);

    $conn->select($db);

    $logger->trace("Incrementing key '$key'");
    my $value = $conn->incr($key);
    $logger->trace("Value: $value");

    return $value;
}

sub lpush($$@) {
    my ($db, $key, @values) = @_;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless($key);

    $conn->select($db);

    $logger->debug("Left Push on key '$key'");
    $conn->lpush($key, @values);
    $logger->trace("LPUSH complete");

    return;
}

sub set($$$) {
    my ($db, $key, $value) = @_;
    my $conn = connection();
    my $logger = Log::Log4perl->get_logger();

    return undef unless($key);

    $conn->select($db);

    $logger->trace("Setting key '$key' => $value");
    return $conn->set($key, $value);
}

1;

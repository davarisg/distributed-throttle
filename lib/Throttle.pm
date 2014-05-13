package Throttle;

use strict;
use warnings;

use Data::Dumper ();
use Utils::Redis ();
use Utils::Semaphore ();
use Log::Log4perl ();
use Time::HiRes ();

=head1 NAME
Throttle - A Distributed Throttling module

=head1 AUTHOR
Georgios Davaris

=head1 SYNOPSIS
The purpose of this module is to implement throttling 

=head1 EXAMPLE
# Required arguments:

my $t = Throttle->new(
    'rpm'       => 60,
);

# Available parameters:
#   'count_key'         => Redis key that holds total count of operations completed (Defaults to 't:count')
#   'redis_db'          => Redis DB where keys will be stored (Defaults to 0)
#   'reset_threshold'   => Counter will reset to 0 after it reaches the threshold (Defaults to 1000000)
#   'rpm'               => Requests per minute
#   'timer_key'         => Redis key that hold the value of the timer (Defaults to 't:timer')

# The throttle() function will block until enough time has passed
# (based on what 'rpm' was set to) for the next operation / request
# to go through.

$t->throttle();

=cut

# Dynamically create getters and setters for the following PROPERTIES
BEGIN {
    my %PROPERTIES = (
        'count_key'         => 't:count',
        'redis_db'          => 0,
        'reset_threshold'   => 1000000,
        'rpm'               => undef,
        'semaphore'         => undef,
        'timer'             => Time::HiRes::time,
        'timer_key'         => 't:timer',
    );

    foreach my $key (keys(%PROPERTIES)) {
        eval "
        sub $key (\$;\$) {
            my (\$self, \$set) = \@_;
            if (defined(\$set)) {
                \$self->{'$key'} = \$set;
            }
            if (defined(\$self->{'$key'})) {
                return \$self->{'$key'};
            }
            return \$PROPERTIES{\$key};
        }";
    }
}

sub new($) {
    my $class = shift(); 
    my $logger = Log::Log4perl->get_logger();

    my $self = {
        @_,
    };

    my $semaphore = Utils::Semaphore->new(
        'count'     => 1,
        'db'        => $self->{'redis_db'} || 0,
        'key'       => $self->{'mutex_key'} || 's:mutex',
        'key_init'  => $self->{'mutex_key_init'} || 's:mutex_init',
    );

    $self->{'semaphore'} = $semaphore;

    # Error Checking: Make sure 'rpm' is always defined
    if (!$self->{'rpm'}) {
        $logger->error('"rpm" not defined');
        return undef;
    }

    bless($self, $class);
    return $self;
}

# This function Will cleanup mutex, counts and timer
# from previous run
sub cleanup($) {
    my $self = $_[0];
    my $logger = Log::Log4perl->get_logger();

    my $count_key = $self->count_key();
    my $semaphore = $self->semaphore();
    my $timer_key = $self->timer_key();

    # Cleanup semaphore keys
    $semaphore->cleanup();

    # Clean the count key
    Utils::Redis::del(0, $count_key, $timer_key);

    return;
}

# Call this function before the operation you want throttled.
# It will sleep for the required amount of time.
sub throttle($) {
    my $self = $_[0];
    my $logger = Log::Log4perl->get_logger();

    # Grab all the necessary variables
    my $count_key = $self->count_key();
    my $timer_key = $self->timer_key();
    my $db = $self->redis_db();
    my $reset_threshold = $self->reset_threshold();
    my $rpm = $self->rpm();
    my $semaphore = $self->semaphore();

    # Acquire the semaphore
    $semaphore->acquire();

    # Figure out what our previous window was.
    my $c = Utils::Redis::get($db, $count_key) || 0;
    my $timer = Utils::Redis::get($db, $timer_key) || $self->timer();
    my $rps = $rpm / 60;
    my $window = Time::HiRes::time - $timer;

    # Check if it is time to reset counter
    if ($c >= $reset_threshold * $rpm) {
        Utils::Redis::set($db, $count_key, 0);
        $logger->debug('Resetting counter');
    }

    $logger->trace(sprintf("Calls made: %s", $c));
    $logger->trace(sprintf("Window since last operation: %s", $window));

    # Is it time to throttle?
    if ($window < 1 / $rps) {
        my $sleep_time = (1 / $rps) - $window;
        $logger->trace("Throttling for $sleep_time seconds");
        Time::HiRes::sleep($sleep_time);
    }

    # Increment the call count and update the timer
    Utils::Redis::incr($db, $count_key);
    Utils::Redis::set($db, $timer_key, Time::HiRes::time);

    # Relase the semaphore
    $semaphore->release();
}

1;

__END__

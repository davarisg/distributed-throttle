package Utils::Semaphore;

use Log::Log4perl ();
use Utils::Redis ();


=head1 NAME
Utils::Semaphore - Implements a distributed semaphore

=head1 AUTHOR
Georgios Davaris

=head1 SYNOPSIS
This module implements a distributed semaphore for the Throttle module.
A lock is acquired with Redis BLPOP which blocks until it finds an element
to pop from the given list. The lock is released with LPUSH which left pushes an
element in the given key.

=head1 EXAMPLE
# Required arguments:

my $semaphore = Utils::Semaphore->new(
    'count'     => 1,
    'db'        => 0,
    'key'       => 's:mutex',
    'key_init'  => 's:mutex_init',
);

# Available parameters:
#   'count'     => Number of processes that can acquire the semaphore at the same time
#   'db'        => Redis DB where keys will be stored
#   'key'       => Semaphore Redis key
#   'key_init'  => Semaphore Redis initialization key

# All of the above parameteres are required for this to work. There are no
# default parameters available.

# There are 3 functions meant to be used externally:

# Tries to acquire the semaphore and blocks until it succeeds
$semaphore->acquire();

# Releases the semaphore if the current release count does not exceed the given count
$semaphore->release();

# Clean-up Redis keys for this semaphore
$semaphore->cleanup();

=cut


# Dynamically create getters and setters for the following PROPERTIES
BEGIN {
    my %PROPERTIES = (
        'count'         => 0,
        'db'            => undef,
        'key'           => '',
        'key_init'      => '',
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
        @_
    };

    # Track the number of times the semaphore has been released
    $self->{'release'} = $self->{'count'};

    # Error Checking: Is 'count' defined? 
    if (!exists($self->{'count'})) {
        $logger->warn('No count given for Semaphore');
        return undef;
    }

    # Is 'db' defined?
    if (!exists($self->{'db'})) {
        $logger->warn('No database set for Semaphore keys');
        return undef;
    }

    # Is 'key' defined?
    if (!exists($self->{'key'})) {
        $logger->warn('No Semaphore key defined');
        return undef;
    }

    # Is 'key_init' defined?
    if (!exists($self->{'key_init'})) {
        $logger->warn('No Semaphore key_init defined');
        return undef;
    }

    bless($self, $class);
    return $self;
}

# This function gets called when the semaphore has to be acquired.
# It does a BLPOP operation in Redis and blocks until it can grab
# key.
sub acquire($) {
    my $self = $_[0];
    my $db = $self->db();
    my $key = $self->key();
    my $logger = Log::Log4perl->get_logger();

    $self->initialize();

    $logger->trace("Acquiring lock on key '$key'");
    my $value = Utils::Redis::blpop($db, $key);
    $logger->debug('Lock acquired');
    $logger->trace({
        'filter'        => \&Data::Dumper::Dumper,
        'value'         => $value,
    });

    $self->{'release'}--;
    return $value;
}

# This function will delete all of the Redis keys used by this
# semaphore if called externally
sub cleanup($) {
    my $self = $_[0];
    my $db = $self->db();
    my $key = $self->key();
    my $key_init = $self->key_init();
    my $logger = Log::Log4perl->get_logger();

    Utils::Redis::del($db, ($key, $key_init));
}

# This function gets called once the first time the acquire
# function gets called.
# It checks if the sempahore has already been initialized and
# if not it initializes it.
sub initialize($) {
    my $self = $_[0];
    my $count = $self->count();
    my $db = $self->db();
    my $key = $self->key();
    my $key_init = $self->key_init();
    my $logger = Log::Log4perl->get_logger();

    # If this function was already called once return
    return $self->{'initialize'} if ($self->{'initialize'});

    # Initialize the semaphore key if it hasn't been initialized
    if (Utils::Redis::getset($db, $key_init, 1) != 1) {
        $logger->debug("Initializing Semaphore key '$key'");
        return undef unless (Utils::Redis::lpush($db, $key, $count));
    }

    # Ensure that the function will not be called again
    $self->{'initialize'} = 1;
    return;
}

# This function will release the lock by performing an LPUSH
# in Redis.
# If the semaphore is a mutex it will not release the lock
# if the lock was already released.
sub release($) {
    my $self = $_[0];
    my $db = $self->db();
    my $key = $self->key();
    my $logger = Log::Log4perl->get_logger();

    # Make sure we do not release more than we can have
    if ($self->count() eq $self->{'release'}) {
        $logger->warn("Cannot release lock. Already released $count times");
        return undef;
    }

    $logger->trace("Releasing lock on key '$key'");
    Utils::Redis::lpush($db, $key, 1);
    $logger->debug('Semaphore Released');

    $self->{'release'}++;
    return;
}

1;

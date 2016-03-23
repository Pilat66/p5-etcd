package Etcd::Keys;

use namespace::autoclean;

use Etcd::Response;
use Try::Tiny;
use Scalar::Util qw(blessed);
use Carp qw(croak);

use Moo::Role;
use Types::Standard qw(Str);

requires qw(version_prefix api_exec);

has _keys_endpoint => ( is => 'lazy', isa => Str );
sub _build__keys_endpoint {
    shift->version_prefix . '/keys';
}

sub try_call{
  my $self = shift;
  my $sub = shift;
  my @arg = @_; # try {} clean @_
  my $res;
  try{
      $self->$sub(@arg);
      $res = 1;
  }
  catch {
    
  };
  return $res;
}

sub set {
    my ($self, $key, $value, %args) = @_;
    croak 'usage: $etcd->set($key, $value, [%args])' if grep { !defined } ($key, $value);
    Etcd::Response->new_from_http($self->api_exec($self->_keys_endpoint.$key, 'PUT', %args, value => $value));
}

sub try_set {
  my $self = shift;
  return $self->try_call('set', @_);
}

sub get {
    my ($self, $key, %args) = @_;
    croak 'usage: $etcd->get($key, [%args])' if !defined $key;
    Etcd::Response->new_from_http($self->api_exec($self->_keys_endpoint.$key, 'GET', %args));
}

sub delete {
    my ($self, $key, %args) = @_;
    croak 'usage: $etcd->delete($key, [%args])' if !defined $key;
    Etcd::Response->new_from_http($self->api_exec($self->_keys_endpoint.$key, 'DELETE', %args));
}

sub try_delete {
  my $self = shift;
  return $self->try_call('delete', @_);
}


sub compare_and_swap {
    my ($self, $key, $value, $prev_value, %args) = @_;
    croak 'usage: $etcd->compare_and_swap($key, $value, $prev_value, [%args])' if grep { !defined } ($key, $value, $prev_value);
    $self->set($key, $value, %args, prevValue => $prev_value);
}

sub try_compare_and_swap {
  my $self = shift;
  return $self->try_call('compare_and_swap', @_);
}


sub compare_and_swap_ex {
    my ($self, $key, $value, %args) = @_;
    croak 'usage: $etcd->compare_and_swap_ex($key, $value, [%args])' if grep { !defined } ($key, $value);
    $self->set($key, $value, %args,);
}

sub try_compare_and_swap_ex {
  my $self = shift;
  return $self->try_call('compare_and_swap_ex', @_);
}


sub compare_and_swap_if_exists {
    my ($self, $key, $value, %args) = @_;
    return $self->compare_and_swap_ex($key, $value, prevExtst=>'true', %args);
}

sub try_compare_and_swap_if_exists {
  my $self = shift;
  return $self->try_call('compare_and_swap_if_exists', @_);
}

sub compare_and_swap_unless_exists {
    my ($self, $key, $value, %args) = @_;
    return $self->compare_and_swap_ex($key, $value, prevExtst=>'false', %args);
}

sub try_compare_and_swap_unless_exists {
  my $self = shift;
  return $self->try_call('compare_and_swap_unless_exists', @_);
}



sub compare_and_delete {
    my ($self, $key, $prev_value, %args) = @_;
    croak 'usage: $etcd->compare_and_delete($key, $prev_value, [%args])' if grep { !defined } ($key, $prev_value);
    $self->delete($key, %args, prevValue => $prev_value);
}

sub try_compare_and_delete {
  my $self = shift;
  return $self->try_call('compare_and_delete', @_);
}


sub create {
    my ($self, $key, $value, %args) = @_;
    croak 'usage: $etcd->create($key, $value, [%args])' if grep { !defined } ($key, $value);
    $self->set($key, $value, %args, prevExist => 'false');
}

sub try_create {
  my $self = shift;
  return $self->try_call('create', @_);
}


sub update {
    my ($self, $key, $value, %args) = @_;
    croak 'usage: $etcd->update($key, $value, [%args])' if grep { !defined } ($key, $value);
    $self->set($key, $value, %args, prevExist => 'true');
}

sub try_update {
  my $self = shift;
  return $self->try_call('update', @_);
}


sub exists {
    my ($self, $key, %args) = @_;
    croak 'usage: $etcd->exists($key, [%args])' if !defined $key;
    try {
        $self->get($key, %args);
        1;
    }
    catch {
        die $_ unless defined blessed $_ && $_->isa('Etcd::Error');
        die $_ unless $_->error_code == 100;
        "";
    }
}

sub create_dir {
    my ($self, $key, %args) = @_;
    croak 'usage: $etcd->create_dir($key, [%args])' if !defined $key;
    Etcd::Response->new_from_http($self->api_exec($self->_keys_endpoint.$key, 'PUT', %args, dir => 'true'));
}

sub try_create_dir {
  my $self = shift;
  return $self->try_call('create_dir', @_);
}


sub delete_dir {
    my ($self, $key, %args) = @_;
    croak 'usage: $etcd->delete_dir($key, [%args])' if !defined $key;
    $self->delete($key, %args, dir => 'true');
}

sub try_delete_dir {
  my $self = shift;
  return $self->try_call('delete_dir', @_);
}



sub create_in_order {
    my ($self, $key, $value, %args) = @_;
    croak 'usage: $etcd->create_in_order($key, $value, [%args])' if grep { !defined } ($key, $value);
    Etcd::Response->new_from_http($self->api_exec($self->_keys_endpoint.$key, 'POST', %args, value => $value));
}

sub try_create_in_order {
  my $self = shift;
  return $self->try_call('create_in_order', @_);
}


sub watch {
    my ($self, $key, %args) = @_;
    croak 'usage: $etcd->watch($key, [%args])' if !defined $key;
    $self->get($key, %args, wait => 'true');
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Etcd::Keys - etcd key space API

=head1 SYNOPSIS

    use Etcd;
    my $etcd = Etcd->new;
    
    # set value for key
    $etcd->set("/message", "hello world");
    $etcd->try_set("/message", "hello world");
    
    # get key
    my $response = $etcd->get("/message");
    
    # delete key
    $etcd->delete("/message");
    $etcd->try_delete("/message");
    
    # atomic compare-and-swap value for key
    $etcd->compare_and_swap("/message", "new", "old");
    $etcd->try_compare_and_swap("/message", "new", "old");
    
    # atomic compare-and-swap value for key with additional parameters
    $etcd->compare_and_swap_ex("/message", "new",);
    $etcd->try_compare_and_swap_ex("/message", "new",);
    
    # atomic compare-and-swap prevExist=>true
    $etcd->compare_and_swap_if_exists("/message", "new",);
    $etcd->try_compare_and_swap_if_exists("/message", "new",);

    # atomic compare-and-swap prevExist=>false
    $etcd->compare_and_swap_unless_exists("/message", "new",);
    $etcd->try_compare_and_swap_unless_exists("/message", "new",);
    
    # atomic compare-and-delete key
    $etcd->compare_and_delete("/message", "old");
    
    # create key. like set, but fails if the key exists
    $etcd->create("/message", "value");

    # create key. like create(), but returns false if the key exists - NOT raise exception
    if($etcd->try_create("/message", "value")){
      say "Ok";
    }
    else {
      say "Failed";
    }
    
    # update key. like set, but fails if the key doesn't exist
    $etcd->update("/message", "value");
    $etcd->try_update("/message", "value");
    
    # check if key exists
    my $exists = $etcd->exists("/message");
    
    # create dir, a "valueless" key to hold subkeys
    $etcd->create_dir("/dir");
    $etcd->try_create_dir("/dir");
    
    # delete key and everything under it
    $etcd->delete_dir("/dir");
    $etcd->try_delete_dir("/dir");
    
    # atomically create in-order key
    $etcd->create_in_order("/dir", "value");
    $etcd->try_create_in_order("/dir", "value");
    
    # block until key changes
    $etcd->watch("/message");

=head1 DESCRIPTION

This module provides access to etcd's key space API. The methods here map
almost exactly to operations described in the etcd API documentation. See
L<Etcd/SEE ALSO> for further reading.

All methods except C<exists> returns a L<Etcd::Response> object on success and
C<die> on error. On error, C<$@> will contain either a reference to a
L<Etcd::Error> object (for API-level errors) or a regular string (for network,
transport or other errors).

Many methods has a 'try_*' synonym. It return true on success and false on error,
in contrast to standard methods. The difference between them can be seen in this example:

  # '/message' key' does not exists
  try {
    $etcd->C<update>('/message', 'new'); 
  }
  catch {
    warn $_;
  }

  unless( $etcd->C<try_update>('/message', 'new'){
    warn "'/message' key' does not exists";
  }

All methods can take any number of additional arguments in C<key =E<gt> value>
form. These are added to the query parameters in the URL that gets submitted to
etcd. This is how you would pass options for C<ttl> or C<recursive>, for
example.

Any arguments of this kind that clash with the internal operation of a method
will silently be ignored; for example, passing a C<value> key to C<set> will be
ignored because that's how the value is passed internally.

=head1 METHODS

=over 4

=item *

C<set>
C<try_set>

    $etcd->set("/message", "hello world");
    $etcd->try_set("/message", "hello world");

Set a value for a key. The key will be created if it doesn't exist.

This invokes the C<PUT> method for the given key.

=item *

C<get>

    my $node = $etcd->get("/message");

Get a key.

This invokes the C<GET> method for the given key.

=item *

C<delete>
C<try_delete>

    $etcd->delete("/message");
    $etcd->try_delete("/message");

Delete a key.

This invokes the C<DELETE> method for the given key.

=item *

C<compare_and_swap>
C<try_compare_and_swap>

    $etcd->compare_and_swap("/message", "new", "old");
    $etcd->try_compare_and_swap("/message", "new", "old");

Atomic compare-and-swap the value of a key.

This invokes the C<PUT> method for the given key with the C<prevValue> query
parameter.


=item *

C<compare_and_swap_ex>
C<try_compare_and_swap_ex>

    $etcd->compare_and_swap_ex("/message", "new", [%arg]);
    $etcd->try_compare_and_swap_ex("/message", "new", [%arg]);

Atomic compare-and-swap the value of a key.

This invokes the C<PUT> method for the given key with any additional parameters - prevExist, as example.


=item *

C<compare_and_swap_if_exists>
C<try_compare_and_swap_if_exists>

    $etcd->compare_and_swap_if_exists("/message", "new", [%arg]);
    $etcd->try_compare_and_swap_if_exists("/message", "new", [%arg]);

Atomic compare-and-swap the value of a key with prevExist=>true

This invokes the C<PUT> method for the given key with additional parameters - prevExist=>true.


=item *

C<compare_and_swap_unless_exists>
C<try_compare_and_swap_unless_exists>

    $etcd->compare_and_swap_unless_exists("/message", "new", [%arg]);
    $etcd->try_compare_and_swap_unless_exists("/message", "new", [%arg]);

Atomic compare-and-swap the value of a key with prevExist=>false

This invokes the C<PUT> method for the given key with additional parameters - prevExist=>false.

=item *

C<compare_and_delete>
C<try_compare_and_delete>

    $etcd->compare_and_delete("/message", "old");
    $etcd->try_compare_and_delete("/message", "old");

Atomic compare-and-delete the value of a key.

This invokes the C<DELETE> method for the given key with the C<prevValue> query
parameter.

=item *

C<create>
C<try_create>

    $etcd->create("/message", "value");
    $etcd->try_create("/message", "value");

Create a key. Like set, but fails if the key exists.

This invokes the C<PUT> method for the given key with the C<prevExist> query
parameter set to C<false>.

=item *

C<update>
C<try_update>

    $etcd->create("/message", "value");
    $etcd->try_create("/message", "value");

Update the value of a key. Like set, but fails if the key doesn't exist.

This invokes the C<PUT> method for the given key with the C<prevExist> query
parameter set to C<true>.

=item *

C<exists>

    my $exists = $etcd->exists("/message");

Check if key exists. Unlike the other methods, it does not return a reference
to a L<Etcd::Response> object but insteads returns a true or false value. It
may still throw an error.

This invokes the C<GET> method for the given key.

=item *

C<create_dir>
C<try_create_dir>

    $etcd->create_dir("/dir");
    $etcd->try_create_dir("/dir");

Creates a directory, a "valueless" key to hold sub-keys.

This invokes the C<PUT> method for the given key with the C<dir> query
parameter set to C<true>.

=item *

C<delete_dir>
C<try_delete_dir>

    $etcd->delete_dir("/dir");
    $etcd->try_delete_dir("/dir");

Deletes a key and all its sub-keys.

This invokes the C<DELETE> method for the given key with the C<dir> query
parameter set to C<true>.

=item *

C<create_in_order>
C<try_create_in_order>

    $etcd->create_in_order("/dir", "value");
    $etcd->try_create_in_order("/dir", "value");

Atomically creates an in-order key.

This invokes the C<POST> method for the given key.

=item *

C<watch>

    $etcd->watch("/message");

Block until the given key changes, then return the change.

This invokes the C<GET> method for the given key with the C<wait> query
parameter set to C<true>.

=back

=head1 KNOWN ISSUES

=over 4

=item *

There is no convenient way to specify the C<prevIndex> test to
C<compare_and_swap> or C<compare_and_delete>. These can be implemented directly
with C<set>.

=item *

C<watch> has no asynchronous mode.

=back

See L<Etcd/SUPPORT> for information on how to report bugs or feature requests.

=head1 AUTHORS

=over 4

=item *

Robert Norris <rob@eatenbyagrue.org>

=back

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2014 by Robert Norris.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

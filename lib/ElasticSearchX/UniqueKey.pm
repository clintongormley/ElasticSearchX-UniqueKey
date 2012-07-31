package ElasticSearchX::UniqueKey;

use strict;
use warnings;
use Carp;

#===================================
sub new {
#===================================
    my $class  = shift;
    my %params = (
        index => 'unique_key',
        ref $_[0] ? %{ shift() } : @_
    );
    my $self = bless {}, $class;
    for (qw(index es)) {
        $self->{"_$_"} = $params{$_}
            or croak "Missing required param $_";
    }
    return $self;
}

#===================================
sub create {
#===================================
    my $self = shift;
    my %params = $self->_params( 'create', @_ );

    eval {
        $self->es->create( %params, data => {} );
        1;
    }
        && return 1;
    return 0 if $@->isa('ElasticSearch::Error::Conflict');
    croak $@;
}

#===================================
sub delete {
#===================================
    my $self = shift;
    my %params = $self->_params( 'delete', @_ );
    $self->es->delete( %params, ignore_missing => 1 );
}

#===================================
sub exists {
#===================================
    my $self = shift;
    my %params = $self->_params( 'exists', @_ );
    $self->es->exists(%params);
}

#===================================
sub update {
#===================================
    my $self   = shift;
    my %params = $self->_params( 'update', shift(), shift() );
    my $new_id = shift();
    croak "No new id passed to update()"
        unless defined $new_id and length $new_id;

    my ( $type, $old_id ) = @params{ 'type', 'id' };
    $self->create( $type, $new_id )
        and $self->delete( $type, $old_id )
        || carp ("Unique key $type/$old_id not found") && 1;
}

#===================================
sub _params {
#===================================
    my ( $self, $method, $type, $id ) = @_;
    croak "No type passed to ${method}()"
        unless defined $type and length $type;
    croak "No id passed to ${method}()"
        unless defined $id and length $id;

    return (
        index => $self->index,
        type  => $type,
        id    => $id
    );
}

#===================================
sub bootstrap {
#===================================
    my $self = shift;
    my %params = ref $_[0] eq 'HASH' ? %{ shift() } : @_;
    %params = (
        auto_expand_replicas => '0-all',
        number_of_shards     => 1,
    ) unless %params;

    my $es    = $self->es;
    my $index = $self->index;
    return if $es->index_exists( index => $index );

    $es->create_index(
        index    => $index,
        settings => \%params,
        mappings => {
            _default_ => {
                _all    => { enabled => 0 },
                _source => { enabled => 0 },
                _type   => { index   => 'no' },
                enabled => 0,
            }
        }
    );
    $es->cluster_health(wait_for_status=>'yellow');
    return $self;
}

#===================================
sub index { shift->{_index} }
sub es    { shift->{_es} }
#===================================

#===================================
sub delete_type {
#===================================
    my $self = shift;
    my $type = shift;
    croak "No type passed to delete_type()"
        unless defined $type and length $type;

    $self->es->delete_mapping(
        index          => $self->index,
        type           => $type,
        ignore_missing => 1
    );
    return $self;
}

#===================================
sub delete_index {
#===================================
    my $self = shift;
    $self->es->delete_index( index => $self->index, ignore_missing => 1 );
    return $self;
}

1;

# ABSTRACT: Track unique keys in ElasticSearch

=head1 DESCRIPTION

The only unique key available in ElasticSearch is the document ID. Typically,
if you want a document to be unique, you use the unique value as the ID.
However, sometimes you don't want to do this. For instance, you may want
to use the email address as a unique identifier for your user accounts, but
you also want to be able to link to a user account without exposing their email
address.

L<ElasticSearchX::UniqueKey> allows you to keep track of unique values by
maintaining a dedicated index which can contain multiple C<types>.  Each
C<type> represents a different key name (so a single index can be used
to track multiple unique keys).


=head1 SYNOPSIS

    use ElasticSearch();
    use ElasticSearchX::UniqueKey();

    my $es   = ElasticSearch->new();
    my $uniq = ElasticSearchX::UniqueKey->new( es => $es );

    $uniq->bootstrap();

    $created = $uniq->create( $key_name, $key_id );
    $deleted = $uniq->delete( $key_name, $key_id );
    $exists  = $uniq->exists( $key_name, $key_id );
    $updated = $uniq->update( $key_name, $old_id, $new_id );


    $uniq->delete_index;
    $uniq->delete_type( $key_name );

=head1 METHODS

=head2 new()

    my uniq = ElasticSearchX::UniqueKey->new(
        es      => $es,         # ElasticSearch instance, required
        index   => 'index',     # defaults to 'unique_key',
    );

C<new()> returns a new instance of L<ElasticSearchX::UniqueKey>. The unique
keys are stored in the specified index, which is setup to be very efficient
for this purpose, but not useful for general storage.

You must call L</bootstrap()> to create your index before first using it,
otherwise it will not be setup correctly.
See L</"bootstrap()"> for how to initiate your index.

You don't need to setup your C<key_names> (ie your C<types>) - these will
be created automatically.

=head2 create()

    $created = $uniq->create( $key_name, $key_id );

Returns true if the C<key_name/key_id> combination didn't already exist and
it has been able to create it.  Returns false if it already exists.

=head2 delete()

    $deleted = $uniq->delete( $key_name, $key_id );

Returns true if the C<key_name/key_id> combination existed and it has been
able to delete it. Returns false if it didn't already exist.

=head2 exists()

    $exists = $uniq->exists( $key_name, $key_id );

Returns true or false depending on whether the C<key_name/key_id> combination
exists or not.

=head2 update()

    $updated = $uniq->update( $key_name, $old_id, $new_id );

First tries to create the new combination C<key_name/new_id>, otherwise
returns false.  Once created, it then tries to delete the
C<key_name/old_id>, and returns true regardless of whether it existed previously
or not. It will warn if the old combination didn't exist.


=head2 bootstrap()

    $uniq->bootstrap( %settings );

This method will create the index, if it doesn't already exist.
By default, the index is setup with the following C<%settings>:

    (
        number_of_shards     => 1,
        auto_expand_replicas => '0-all',
    )

In other words, it will have only a single primary shard (instead of the
ElasticSearch default of 5), and a replica of that shard on every ElasticSearch
node in your cluster.

If you pass in any C<%settings> then the defaults will not be used at all.

See L<Index Settings|http://www.elasticsearch.org/guide/reference/api/admin-indices-update-settings.html> for more.

=head2 delete_index()

    $uniq->delete_index()

Deletes the index. B<You will lose your data!>

=head2 delete_type()

    $uniq->delete_type( $key_name )

Deletes the type associated with the C<key_name>. B<You will lose your data!>

=head2 index()

    $index = $uniq->index

Read-only getter for the index value

=head2 es()

    $es = $uniq->es

Read-only getter for the ElasticSearch instance.

=head1 SEE ALSO

=over

=item L<ElasticSearch>

=item L<Elastic::Model>

=item L<http://www.elasticsearch.org>

=back

=head1 BUGS

This is a new module, so there will probably be bugs, and the API may change
in the future.

If you have any suggestions for improvements, or find any bugs, please
report them to http://github.com/clintongormley/ElasticSearchX-UniqueKey/issues. I
will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 TEST SUITE

The full test suite requires a live ElasticSearch cluster to run.  CPAN
testers doesn't support this.  You can see full test results here:
L<http://travis-ci.org/#!/clintongormley/ElasticSearchX-UniqueKey/builds>.

To run the full test suite locally, run it as:

    perl Makefile.PL
    ES_HOME=/path/to/elasticsearch make test

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc ElasticSearchX::UniqueKey

You can also look for information at:

=over

=item * GitHub

L<http://github.com/clintongormley/ElasticSearchX-UniqueKey>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/ElasticSearchX-UniqueKey>

=item * Search MetaCPAN

L<https://metacpan.org/module/ElasticSearchX::UniqueKey>

=back


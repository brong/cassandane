#!/usr/bin/perl
#
#  Copyright (c) 2011 Opera Software Australia Pty. Ltd.  All rights
#  reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#
#  1. Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
#
#  3. The name "Opera Software Australia" must not be used to
#     endorse or promote products derived from this software without
#     prior written permission. For permission or any legal
#     details, please contact
# 	Opera Software Australia Pty. Ltd.
# 	Level 50, 120 Collins St
# 	Melbourne 3000
# 	Victoria
# 	Australia
#
#  4. Redistributions of any form whatsoever must retain the following
#     acknowledgment:
#     "This product includes software developed by Opera Software
#     Australia Pty. Ltd."
#
#  OPERA SOFTWARE AUSTRALIA DISCLAIMS ALL WARRANTIES WITH REGARD TO
#  THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
#  AND FITNESS, IN NO EVENT SHALL OPERA SOFTWARE AUSTRALIA BE LIABLE
#  FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
#  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
#  AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
#  OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

use strict;
use warnings;
package Cassandane::Cyrus::TestCase;
use base qw(Cassandane::Unit::TestCase);
use Cassandane::Util::Log;
use Cassandane::Generator;
use Cassandane::MessageStoreFactory;
use Cassandane::Instance;

my @stores = qw(store adminstore replica_store replica_adminstore);

sub new
{
    my ($class, $params, @args) = @_;
    my $port;

    my $want = {
	instance => 1,
	replica => 0,
	services => [ 'imap' ],
	store => 1,
	adminstore => 0,
	gen => 1,
    };
    # This is a downright dirty hack; if the test name contains the word
    # 'replication' then enable the replica unless requested otherwise
    if (defined $args[0] && grep { m/^replication$/ } split(/_/,$args[0]))
    {
	xlog "magically enabling replica because test name contains 'replication'";
	$want->{replica} = 1;
    }
    map {
	$want->{$_} = $params->{$_}
	    if defined $params->{$_};
    } keys %$want;
    $want->{folder} = $params->{folder}
	if defined $params->{folder};

    my %instance_params;
    foreach my $p (qw(config))
    {
	$instance_params{$p} = $params->{$p}
	    if defined $params->{$p};
    }

    my $self = $class->SUPER::new(@args);
    $self->{_want} = $want;

    if ($want->{instance})
    {
	if ($want->{replica})
	{
	    $port = Cassandane::Service->alloc_port();
	    my $conf = $instance_params{config} || Cassandane::Config->default();
	    $conf = $conf->clone();
	    $conf->set(
		# sync_client will find the port in the config
		sync_port => $port,
		# tell sync_client how to login
		sync_authname => 'repluser',
		sync_password => 'replpass',
		sync_realm => 'internal',
		sasl_mech_list => 'PLAIN',
		# Ensure sync_server gives sync_client enough privileges
		admins => 'admin repluser',
	    );
	    $instance_params{config} = $conf;
	}

	$self->{instance} = Cassandane::Instance->new(%instance_params);
	$self->{instance}->add_services(@{$want->{services}});

	if ($want->{replica})
	{
	    $self->{replica} = Cassandane::Instance->new(%instance_params,
						         setup_mailbox => 0);
	    $self->{replica}->add_service('sync', port => $port);
	    $self->{replica}->add_services(@{$want->{services}});
	}
    }

    if ($want->{gen})
    {
	$self->{gen} = Cassandane::Generator->new();
    }

    return $self;
}

sub set_up
{
    my ($self) = @_;

    $self->{instance}->start()
	if (defined $self->{instance});
    $self->{replica}->start()
	if (defined $self->{replica});

    $self->{store} = undef;
    $self->{adminstore} = undef;
    $self->{master_store} = undef;
    $self->{master_adminstore} = undef;
    $self->{replica_store} = undef;
    $self->{replica_adminstore} = undef;

    # Run the replication engine to create the user mailbox
    # in the replica.  Doing it this way avoids issues with
    # mismatched mailbox uniqueids.
    $self->run_replication()
	if (defined $self->{replica});

    my %store_params;
    $store_params{folder} = $self->{_want}->{folder}
	if defined $self->{_want}->{folder};

    my %adminstore_params = ( %store_params, username => 'admin' );
    # The admin stores need an extra parameter to force their
    # default folder because otherwise they will default to 'INBOX'
    # which refers to user.admin not user.cassandane
    $adminstore_params{folder} ||= 'INBOX';
    $adminstore_params{folder} = 'user.cassandane'
	if ($adminstore_params{folder} =~ m/^inbox$/i);

    if (defined $self->{instance})
    {
	my $svc = $self->{instance}->get_service('imap');
	if (defined $svc)
	{
	    $self->{store} = $svc->create_store(%store_params)
		if ($self->{_want}->{store});
	    $self->{adminstore} = $svc->create_store(%adminstore_params)
		if ($self->{_want}->{adminstore});
	}
    }
    if (defined $self->{replica})
    {
	# aliases for the master's store(s)
	$self->{master_store} = $self->{store};
	$self->{master_adminstore} = $self->{adminstore};

	my $svc = $self->{replica}->get_service('imap');
	if (defined $svc)
	{
	    $self->{replica_store} = $svc->create_store(%store_params)
		if ($self->{_want}->{store});
	    $self->{replica_adminstore} = $svc->create_store(%adminstore_params)
		if ($self->{_want}->{adminstore});
	}
    }
}

sub tear_down
{
    my ($self) = @_;

    foreach my $s (@stores)
    {
	if (defined $self->{$s})
	{
	    $self->{$s}->disconnect();
	    $self->{$s} = undef;
	}
    }
    $self->{master_store} = undef;
    $self->{master_adminstore} = undef;

    if (defined $self->{instance})
    {
	$self->{instance}->stop();
	$self->{instance} = undef;
    }
    if (defined $self->{replica})
    {
	$self->{replica}->stop();
	$self->{replica} = undef;
    }
}

# TODO: provide a way to do this in the same instance
# which would be more efficient
sub restart_with_config
{
    my ($self, %nv) = @_;

    my $conf = $self->{instance}->{config}->clone();
    $conf->set(%nv);

    $self->tear_down();
    $self->{instance} = Cassandane::Instance->new(config => $conf);
    $self->{instance}->add_service('imap');
    $self->set_up();
}

sub _save_message
{
    my ($self, $msg, $store) = @_;

    $store ||= $self->{store};

    $store->write_begin();
    $store->write_message($msg);
    $store->write_end();
}

sub make_message
{
    my ($self, $subject, %attrs) = @_;

    my $store = $attrs{store};	# may be undef
    delete $attrs{store};

    my $msg = $self->{gen}->generate(subject => $subject, %attrs);
    $self->_save_message($msg, $store);

    return $msg;
}

sub check_messages
{
    my ($self, $expected, %params) = @_;
    my $actual = {};
    my $store = $params{store} || $self->{store};

    xlog "check_messages: " . join(' ', %params);

    $store->read_begin();
    while (my $msg = $store->read_message())
    {
	my $subj = $msg->get_header('subject');
	$self->assert(!defined $actual->{$subj});
	$actual->{$subj} = $msg;
    }
    $store->read_end();

    $self->assert(scalar keys %$actual == scalar keys %$expected);

    foreach my $expmsg (values %$expected)
    {
	my $subj = $expmsg->get_header('subject');
	my $actmsg = $actual->{$subj};

	$self->assert_not_null($actmsg);

	xlog "checking guid";
	$self->assert_str_equals($expmsg->get_guid(),
			         $actmsg->get_guid());

	# Check required headers
	foreach my $h (qw(x-cassandane-unique))
	{
	    xlog "checking $h";
	    $self->assert_not_null($actmsg->get_header($h));
	    $self->assert_str_equals($expmsg->get_header($h),
				     $actmsg->get_header($h));
	}

	# if there were optional headers we wished to check, do it here

	# check optional string attributes
	foreach my $a (qw(id uid cid))
	{
	    next unless defined $expmsg->get_attribute($a);
	    xlog "checking $a";
	    $self->assert_str_equals($expmsg->get_attribute($a),
				     $actmsg->get_attribute($a));
	}

	# check optional structured attributes
	foreach my $a (qw(flags))
	{
	    next unless defined $expmsg->get_attribute($a);
	    xlog "checking $a";
	    $self->assert_deep_equals($expmsg->get_attribute($a),
				      $actmsg->get_attribute($a));
	}

	# check annotations
	foreach my $ea ($expmsg->list_annotations())
	{
	    xlog "checking annotation ($ea->{entry} $ea->{attrib})";
	    $self->assert_not_null($actmsg->get_annotation($ea));
	    $self->assert_str_equals($expmsg->get_annotation($ea),
				     $actmsg->get_annotation($ea));
	}
    }

    return $actual;
}

sub run_replication
{
    my ($self) = @_;

    xlog "running replication";

    # Disconnect during replication to ensure no imapd
    # is locking the mailbox, which gives us a spurious
    # error which is ignored in real world scenarios.
    foreach my $s (@stores)
    {
	$self->{$s}->disconnect()
	    if defined $self->{$s};
    }

    my $params =
	$self->{replica}->get_service('sync')->store_params();

    # TODO: need a timeout!!

    $self->{instance}->run_utility('sync_client',
	'-v',			# verbose
	'-v',			# even more verbose
	'-S', $params->{host},	# hostname to connect to
	'-u', 'cassandane',	# replicate the Cassandane user
	);


    foreach my $s (@stores)
    {
	if (defined $self->{$s})
	{
	    $self->{$s}->_connect();
	    $self->{$s}->_select();
	}
    }
}


1;
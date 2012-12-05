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

package Cassandane::Cyrus::Search;
use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use POSIX qw(:errno_h);
use Cassandane::Util::Log;
use Cassandane::Util::Wait;
use Cassandane::Mboxname;
use Data::Dumper;

use lib '.';
use base qw(Cassandane::Cyrus::TestCase);
use Cassandane::Util::Log;

Cassandane::Cyrus::TestCase::magic(sphinx => sub { shift->want(search => 'sphinx'); });
Cassandane::Cyrus::TestCase::magic(xapian => sub { shift->want(search => 'xapian'); });
Cassandane::Cyrus::TestCase::magic(squat => sub { shift->want(search => 'squat'); });
Cassandane::Cyrus::TestCase::magic(xconvmultisort => sub {
    shift->config_set(
	conversations => 'on',
	conversations_db => 'twoskip'
    );
});
Cassandane::Cyrus::TestCase::magic(RollingSquatter => sub {
    shift->config_set(
	sync_log => 'yes',
	sync_log_channels => 'squatter'
    );
});
Cassandane::Cyrus::TestCase::magic(SmallBatchsize => sub {
    shift->config_set(search_batchsize => '3');
});
Cassandane::Cyrus::TestCase::magic(NoIndexHeaders => sub {
    shift->config_set(search_index_headers => 'no');
});

sub new
{
    my $class = shift;
    return $class->SUPER::new({ adminstore => 1 }, @_);
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
}

sub tear_down
{
    my ($self) = @_;
    $self->SUPER::tear_down();
}

sub _fgrep_msgs
{
    my ($msgs, $attr, $s) = @_;
    my @res;

    foreach my $msg (values %$msgs)
    {
	push(@res, $msg->uid())
	    if (index($msg->$attr(), $s) >= 0);
    }
    @res = sort { $a <=> $b } @res;
    return \@res;
}

sub test_from
{
    my ($self) = @_;

    xlog "test SEARCH with the FROM predicate";
    my $talk = $self->{store}->get_client();

    xlog "append some messages";
    my %exp;
    my %from_domains;
    my $N = 20;
    for (1..$N)
    {
	my $msg = $self->make_message("Message $_");
	$exp{$_} = $msg;
	my ($dom) = ($msg->from() =~ m/(@[^>]*)>/);
	$from_domains{$dom} = 1;
	xlog "Message uid " . $msg->uid() . " from domain " . $dom;
    }
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    my @found;
    foreach my $dom (keys %from_domains)
    {
	xlog "searching for: FROM $dom";
	my $uids = $talk->search('from', { Quote => $dom })
	    or die "Cannot search: $@";
	my $expected_uids = _fgrep_msgs(\%exp, 'from', $dom);
	$self->assert_deep_equals($expected_uids, $uids);
	map { $found[$_] = 1 } @$uids;
    }

    xlog "checking all the message were found";
    for (1..$N)
    {
	$self->assert($found[$_],
		      "UID $_ was not returned from a SEARCH");
    }

    xlog "Double-check the messages are still there";
    $self->check_messages(\%exp);
}

sub squat_dump
{
    my ($instance, $mbox) = @_;

    my $filename = $instance->{basedir} . "/squat_dump.out";

    $instance->run_command({
	    cyrus => 1,
	    redirects => { stdout => $filename },
	},
	'squat_dump',
	# we get -C for free
	$mbox
    );

    my $res = {};
    my $mboxname;
    my $uidvalidity;
    open RESULTS, '<', $filename
	or die "Cannot open $filename for reading: $!";
    while ($_ = readline(RESULTS))
    {
	chomp;
	my @a = split;
	if ($a[0] eq 'MAILBOX')
	{
	    $mboxname = $a[1];
	    $res->{$mboxname} ||= {};
	    next;
	}
	elsif ($a[0] eq 'DOC')
	{
	    my ($uidv) = ($a[1] =~ m/^validity\.(\d+)$/);
	    if (defined $uidv)
	    {
		$uidvalidity = 0+$uidv;
		$res->{$mboxname}->{$uidvalidity} ||= {}
	    }
	    else
	    {
		my ($field, $uid) = ($a[1] =~ m/^([mhftcbs])(\d+)$/);
		my $size = $a[2];
		next if !defined $uid;
		$res->{$mboxname}->{$uidvalidity}->{$uid} = 1;
	    }
	}
    }
    close RESULTS;

    return $res;
}

sub test_squatter_squat
{
    my ($self) = @_;

    xlog "test squatter with SQUAT";
    $self->squatter_test_common(\&squat_dump);
}

sub sphinx_socket_path
{
    my ($instance) = @_;

    return $instance->{basedir} . "/conf/socket/sphinx";
}

sub sphinx_dump
{
    my ($instance, $mbin) = @_;

    my $filename = $instance->{basedir} . "/sphinx_dump.out";
    my $sock = sphinx_socket_path($instance);

    return {} if ( ! -e $sock );

    my $mb = $mbin;
    $mb ||= Cassandane::Mboxname->new(
		config => $instance->{config},
		username => 'cassandane');
    $mb = Cassandane::Mboxname->new(
		config => $instance->{config},
		external => $mb)
		unless ref $mb eq 'Cassandane::Mboxname';

    my $indexname = $mb->to_username();
    $indexname =~ s/Z/Z5A/g;
    $indexname =~ s/@/Z40/g;
    $indexname =~ s/\./Z2E/g;
    $indexname =~ s/^/X/;
    xlog "sphinx_dump: indexname \"$indexname\"";

    # First check that the table exists
    $instance->run_command(
	    { redirects => { stdout => $filename } },
	    'mysql',
	    '--socket', $sock,
	    '--batch',
	    '--raw',
	    '-e', "SHOW TABLES"
	    );
    open TABLES, '<', $filename
	or die "Cannot open $filename for reading: $!";
    my $found = 0;
    while ($_ = readline(TABLES))
    {
	chomp;
	my @a = split;
	next if scalar(@a) != 2;
	$found = 1 if ($a[0] eq $indexname);
    }
    close TABLES;
    return {} if !$found;

    $instance->run_command(
	    { redirects => { stdout => $filename } },
	    'mysql',
	    '--socket', $sock,
	    '--batch',
	    '--raw',
	    '-e', "SELECT cyrusid FROM $indexname LIMIT 1000"
	    );

    my $res = {};
    open RESULTS, '<', $filename
	or die "Cannot open $filename for reading: $!";
    while ($_ = readline(RESULTS))
    {
	chomp;
	my @a = split(/\./);
	next if scalar(@a) < 3;
	my $uid = 0 + pop(@a);
	my $uidvalidity = 0 + pop(@a);
	my $mboxname = join('.', @a);
	next if (defined $mbin && $mboxname ne "$mbin");
	$res->{$mboxname} ||= {};
	$res->{$mboxname}->{$uidvalidity} ||= {};
	$res->{$mboxname}->{$uidvalidity}->{$uid} = 1;
    }
    close RESULTS;
    return $res;
}

sub xapian_dump
{
    my ($instance, $mbin) = @_;

    my $filename = $instance->{basedir} . "/xapian_dump.out";

    my $mb = $mbin;
    $mb ||= Cassandane::Mboxname->new(
		config => $instance->{config},
		username => 'cassandane');
    $mb = Cassandane::Mboxname->new(
		config => $instance->{config},
		external => $mb)
		unless ref $mb eq 'Cassandane::Mboxname';

    my $xapiandir = $instance->{basedir} . "/sphinx/" .  $mb->hashed_path() . "/xapian";
    return {} if ( ! -d $xapiandir );

    $instance->run_command(
	    { redirects => { stdout => $filename } },
	    'delve', '-V0', '-1', $xapiandir);

    my $res = {};
    open RESULTS, '<', $filename
	or die "Cannot open $filename for reading: $!";
    while ($_ = readline(RESULTS))
    {
	chomp;
	next if m/^Value 0/;
	s/^\d+://;
	my @a = split(/\./);
	next if scalar(@a) < 3;
	my $uid = 0 + pop(@a);
	my $uidvalidity = 0 + pop(@a);
	my $mboxname = join('.', @a);
	next if (defined $mbin && $mboxname ne "$mbin");
	$res->{$mboxname} ||= {};
	$res->{$mboxname}->{$uidvalidity} ||= {};
	$res->{$mboxname}->{$uidvalidity}->{$uid} = 1;
    }
    close RESULTS;
    return $res;
}

my %dumpers = (
    squat => \&squat_dump,
    sphinx => \&sphinx_dump,
    xapian => \&xapian_dump
);

sub index_dump
{
    my ($instance, $mbin) = @_;
    return $dumpers{$instance->{config}->get('search_engine')}->($instance, $mbin);
}

sub test_squatter_sphinx
    :SmallBatchsize
{
    my ($self) = @_;

    xlog "test squatter with Sphinx";
    $self->squatter_test_common();
}

sub test_squatter_xapian
    :SmallBatchsize
{
    my ($self) = @_;

    xlog "test squatter with Xapian";
    $self->squatter_test_common();
}

sub squatter_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    my $N1 = 10;
    for (1..$N1)
    {
	$exp{$_} = $self->make_message("Message $_");
    }
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Before first index, there is nothing to dump";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({}, $res);

    xlog "First index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the first index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1)
		}
	    }
	}, $res);

    xlog "Second index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "The second run should have no further effect";
    my $res2 = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals($res, $res2);

    xlog "Add another message";
    my $uid = $N1+1;
    $exp{$uid} = $self->make_message("Message $uid");

    xlog "Third index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "The third run should have indexed the new message";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1+1)
		}
	    }
	}, $res);

}

sub test_prefilter_squat
{
    my ($self) = @_;

    xlog "test squatter with Squat";
    $self->prefilter_test_common();
}

sub test_prefilter_sphinx
{
    my ($self) = @_;

    xlog "test squatter with Sphinx";
    $self->prefilter_test_common();
}

sub test_prefilter_xapian
{
    my ($self) = @_;

    xlog "test squatter with Xapian";
    $self->prefilter_test_common();
}

sub run_squatter
{
    my ($instance, @args) = @_;

    my $filename = $instance->{basedir} . "/index_dump.out";

    $instance->run_command({
	    cyrus => 1,
	    redirects => { stdout => $filename },
	},
	'squatter',
	# we get -C for free
	@args
    );

    my $res = {};
    my $mboxname;
    open RESULTS, '<', $filename
	or die "Cannot open $filename for reading: $!";
    while ($_ = readline(RESULTS))
    {
	chomp;
	my @a = split;
	if ($a[0] eq 'mailbox')
	{
	    $mboxname = $a[1];
	    $res->{$mboxname} ||= {};
	    next;
	}
	elsif ($a[0] eq 'uid')
	{
	    $res->{$mboxname}->{$a[1]} = 1;
	}
    }
    close RESULTS;

    return $res;
}

# data thanks to hipsteripsum.me
my @filter_data = (
    {
	body => 'pickled sartorial beer',
	subject => 'umami',
	to => 'viral mixtape',
	from => 'etsy',
	cc => 'cred',
	bcc => 'streetart',
	narwhal => 'butcher',
    },{
	body => 'authentic twee beer',
	subject => 'lomo',
	to => 'cray',
	from => 'artisan mixtape',
	cc => 'beard',
	bcc => 'fannypack',
	narwhal => 'postironic',
    },{
	body => 'vice irony beer',
	subject => 'chambray',
	to => 'chips',
	from => 'banhmi',
	cc => 'dreamcatcher mixtape',
	bcc => 'portland',
	narwhal => 'gentrify',
    },{
	body => 'tattooed twee beer',
	subject => 'ethnic',
	to => 'selvage',
	from => 'forage',
	cc => 'carles',
	bcc => 'shoreditch mixtape',
	narwhal => 'pinterest',
    },{
	body => 'mustache twee beer',
	subject => 'semiotics mixtape',
	to => 'blog',
	from => 'nextlevel',
	cc => 'trustfund',
	bcc => 'austin',
	narwhal => 'tumblr',
    },{
	body => 'williamsburg irony beer',
	subject => 'whatever',
	to => 'gastropub',
	from => 'truffaut',
	cc => 'squid',
	bcc => 'porkbelly',
	narwhal => 'fingerstache mixtape',
    },{
	body => 'organic sartorial beer mixtape',
	subject => 'letterpress',
	to => 'occupuy',
	from => 'cliche',
	cc => 'readymade',
	bcc => 'highlife',
	narwhal => 'artparty',
    },{
	body => 'freegan twee beer',
	subject => 'flexitarian',
	to => 'scenester',
	from => 'bespoke',
	cc => 'salvia',
	bcc => 'godard',
	narwhal => 'helvetica',
    },{
	body => 'ennui sartorial beer',
	subject => 'banksy',
	to => 'aesthetic',
	from => 'jeanshorts',
	cc => 'seitan',
	bcc => 'locavore',
	narwhal => 'hoodie',
    },{
	body => 'quinoa irony beer',
	subject => 'pitchfork',
	to => 'cardigan',
	from => 'brunch',
	cc => 'kogi',
	bcc => 'echopark',
	narwhal => 'messengerbag',
    }
);

sub make_filter_message
{
    my ($self, $d, $uid) = @_;

    my $to = Cassandane::Address->new(
		name => "Test User $d->{to}",
		localpart => 'test',
		domain => 'vmtom.com'
	    );
    my $from = Cassandane::Generator::make_random_address(extra => " $d->{from}");
    my $cc = Cassandane::Generator::make_random_address(extra => " $d->{cc}");
    my $bcc = Cassandane::Generator::make_random_address(extra => " $d->{bcc}");

    my $msg = $self->make_message($d->{subject} . " [$uid]",
				     body => $d->{body} . "\r\n",
				     to => $to,
				     from => $from,
				     cc => $cc,
				     bcc => $bcc,
				     extra_headers => [ [ 'Narwhal', $d->{narwhal} ] ]
				     );
    $msg->set_attribute(uid => $uid);
    return $msg;
}

# expected => [ list of 1-based indexes into @filter_data ]
my @filter_tests = (
    # Test each of the terms which appear in just one message
    { query => 'pickled', expected => [ 1 ] },
    { query => 'authentic', expected => [ 2 ] },
    { query => 'vice', expected => [ 3 ] },
    { query => 'tattooed', expected => [ 4 ] },
    { query => 'mustache', expected => [ 5 ] },
    { query => 'williamsburg', expected => [ 6 ] },
    { query => 'organic', expected => [ 7 ] },
    { query => 'freegan', expected => [ 8 ] },
    { query => 'ennui', expected => [ 9 ] },
    { query => 'quinoa', expected => [ 10 ] },
    { query => 'umami', expected => [ 1 ] },
    { query => 'lomo', expected => [ 2 ] },
    { query => 'chambray', expected => [ 3 ] },
    { query => 'ethnic', expected => [ 4 ] },
    { query => 'semiotics', expected => [ 5 ] },
    { query => 'whatever', expected => [ 6 ] },
    { query => 'letterpress', expected => [ 7 ] },
    { query => 'flexitarian', expected => [ 8 ] },
    { query => 'banksy', expected => [ 9 ] },
    { query => 'pitchfork', expected => [ 10 ] },
    { query => 'viral', expected => [ 1 ] },
    { query => 'cray', expected => [ 2 ] },
    { query => 'chips', expected => [ 3 ] },
    { query => 'selvage', expected => [ 4 ] },
    { query => 'blog', expected => [ 5 ] },
    { query => 'gastropub', expected => [ 6 ] },
    { query => 'occupuy', expected => [ 7 ] },
    { query => 'scenester', expected => [ 8 ] },
    { query => 'aesthetic', expected => [ 9 ] },
    { query => 'cardigan', expected => [ 10 ] },
    { query => 'etsy', expected => [ 1 ] },
    { query => 'artisan', expected => [ 2 ] },
    { query => 'banhmi', expected => [ 3 ] },
    { query => 'forage', expected => [ 4 ] },
    { query => 'nextlevel', expected => [ 5 ] },
    { query => 'truffaut', expected => [ 6 ] },
    { query => 'cliche', expected => [ 7 ] },
    { query => 'bespoke', expected => [ 8 ] },
    { query => 'jeanshorts', expected => [ 9 ] },
    { query => 'brunch', expected => [ 10 ] },
    { query => 'cred', expected => [ 1 ] },
    { query => 'beard', expected => [ 2 ] },
    { query => 'dreamcatcher', expected => [ 3 ] },
    { query => 'carles', expected => [ 4 ] },
    { query => 'trustfund', expected => [ 5 ] },
    { query => 'squid', expected => [ 6 ] },
    { query => 'readymade', expected => [ 7 ] },
    { query => 'salvia', expected => [ 8 ] },
    { query => 'seitan', expected => [ 9 ] },
    { query => 'kogi', expected => [ 10 ] },
    { query => 'streetart', expected => [ 1 ] },
    { query => 'fannypack', expected => [ 2 ] },
    { query => 'portland', expected => [ 3 ] },
    { query => 'shoreditch', expected => [ 4 ] },
    { query => 'austin', expected => [ 5 ] },
    { query => 'porkbelly', expected => [ 6 ] },
    { query => 'highlife', expected => [ 7 ] },
    { query => 'godard', expected => [ 8 ] },
    { query => 'locavore', expected => [ 9 ] },
    { query => 'echopark', expected => [ 10 ] },
    # Test the terms which appear in some but not all messages
    { query => 'sartorial', expected => [ 1, 7, 9 ] },
    { query => 'twee', expected => [ 2, 4, 5, 8 ] },
    { query => 'irony', expected => [ 3, 6, 10 ] },
    # Test the term which appears in all messages
    { query => 'beer', expected => [ 1..10 ] },
    # Test a term which appears in no messages
    { query => 'cosby', expected => [ ] },
    # Test AND of two terms
    { query => '__begin:and pickled authentic __end:and', expected => [ ] },
    { query => '__begin:and twee irony __end:and', expected => [ ] },
    { query => '__begin:and twee mustache __end:and', expected => [ 5 ] },
    { query => '__begin:and quinoa beer __end:and', expected => [ 10 ] },
    { query => '__begin:and twee beer __end:and', expected => [ 2, 4, 5, 8 ] },
    # Test AND of three terms
    { query => '__begin:and pickled tattooed williamsburg __end:and', expected => [ ] },
    { query => '__begin:and quinoa organic beer __end:and', expected => [ ] },
    { query => '__begin:and quinoa irony beer __end:and', expected => [ 10 ] },
    # Test OR of two terms
    { query => '__begin:or pickled authentic __end:or', expected => [ 1, 2 ] },
    { query => '__begin:or twee irony __end:or', expected => [ 2, 3, 4, 5, 6, 8, 10 ] },
    { query => '__begin:or twee mustache __end:or', expected => [ 2, 4, 5, 8 ] },
    { query => '__begin:or quinoa beer __end:or', expected => [ 1..10 ] },
    { query => '__begin:or twee beer __end:or', expected => [ 1..10 ] },
    # Test OR of three terms
    { query => '__begin:or pickled tattooed williamsburg __end:or', expected => [ 1, 4, 6 ] },
    { query => '__begin:or quinoa organic beer __end:or', expected => [ 1..10 ] },
    { query => '__begin:or quinoa irony beer __end:or', expected => [ 1..10 ] },
    # Test each term that appears in the Subject: of just one message
    { query => 'subject:umami', expected => [ 1 ] },
    { query => 'subject:lomo', expected => [ 2 ] },
    { query => 'subject:chambray', expected => [ 3 ] },
    { query => 'subject:ethnic', expected => [ 4 ] },
    { query => 'subject:semiotics', expected => [ 5 ] },
    { query => 'subject:whatever', expected => [ 6 ] },
    { query => 'subject:letterpress', expected => [ 7 ] },
    { query => 'subject:flexitarian', expected => [ 8 ] },
    { query => 'subject:banksy', expected => [ 9 ] },
    { query => 'subject:pitchfork', expected => [ 10 ] },
    # Test each term that appears in the To: of just one message
    { query => 'to:viral', expected => [ 1 ] },
    { query => 'to:cray', expected => [ 2 ] },
    { query => 'to:chips', expected => [ 3 ] },
    { query => 'to:selvage', expected => [ 4 ] },
    { query => 'to:blog', expected => [ 5 ] },
    { query => 'to:gastropub', expected => [ 6 ] },
    { query => 'to:occupuy', expected => [ 7 ] },
    { query => 'to:scenester', expected => [ 8 ] },
    { query => 'to:aesthetic', expected => [ 9 ] },
    { query => 'to:cardigan', expected => [ 10 ] },
    # Test a term that appears in the To: of every message
    { query => 'to:test', expected => [ 1..10 ] },
    # Test each term that appears in the From: of just one message
    { query => 'from:etsy', expected => [ 1 ] },
    { query => 'from:artisan', expected => [ 2 ] },
    { query => 'from:banhmi', expected => [ 3 ] },
    { query => 'from:forage', expected => [ 4 ] },
    { query => 'from:nextlevel', expected => [ 5 ] },
    { query => 'from:truffaut', expected => [ 6 ] },
    { query => 'from:cliche', expected => [ 7 ] },
    { query => 'from:bespoke', expected => [ 8 ] },
    { query => 'from:jeanshorts', expected => [ 9 ] },
    { query => 'from:brunch', expected => [ 10 ] },
    # Test each term that appears in the Cc: of just one message
    { query => 'cc:cred', expected => [ 1 ] },
    { query => 'cc:beard', expected => [ 2 ] },
    { query => 'cc:dreamcatcher', expected => [ 3 ] },
    { query => 'cc:carles', expected => [ 4 ] },
    { query => 'cc:trustfund', expected => [ 5 ] },
    { query => 'cc:squid', expected => [ 6 ] },
    { query => 'cc:readymade', expected => [ 7 ] },
    { query => 'cc:salvia', expected => [ 8 ] },
    { query => 'cc:seitan', expected => [ 9 ] },
    { query => 'cc:kogi', expected => [ 10 ] },
    # Test each term that appears in the Bcc: of just one message
    { query => 'bcc:streetart', expected => [ 1 ] },
    { query => 'bcc:fannypack', expected => [ 2 ] },
    { query => 'bcc:portland', expected => [ 3 ] },
    { query => 'bcc:shoreditch', expected => [ 4 ] },
    { query => 'bcc:austin', expected => [ 5 ] },
    { query => 'bcc:porkbelly', expected => [ 6 ] },
    { query => 'bcc:highlife', expected => [ 7 ] },
    { query => 'bcc:godard', expected => [ 8 ] },
    { query => 'bcc:locavore', expected => [ 9 ] },
    { query => 'bcc:echopark', expected => [ 10 ] },
    # Test each of the terms which appear in the header of just one message
    { query => 'header:butcher', expected => [ 1 ] },
    { query => 'header:postironic', expected => [ 2 ] },
    { query => 'header:gentrify', expected => [ 3 ] },
    { query => 'header:pinterest', expected => [ 4 ] },
    { query => 'header:tumblr', expected => [ 5 ] },
    { query => 'header:fingerstache', expected => [ 6 ] },
    { query => 'header:artparty', expected => [ 7 ] },
    { query => 'header:helvetica', expected => [ 8 ] },
    { query => 'header:hoodie', expected => [ 9 ] },
    { query => 'header:messengerbag', expected => [ 10 ] },
    # Test a term that appears in the header of every message
    { query => 'header:narwhal', expected => [ 1..10 ] },
    # Test each of the terms which appear in the body of just one message
    { query => 'body:pickled', expected => [ 1 ] },
    { query => 'body:authentic', expected => [ 2 ] },
    { query => 'body:vice', expected => [ 3 ] },
    { query => 'body:tattooed', expected => [ 4 ] },
    { query => 'body:mustache', expected => [ 5 ] },
    { query => 'body:williamsburg', expected => [ 6 ] },
    { query => 'body:organic', expected => [ 7 ] },
    { query => 'body:freegan', expected => [ 8 ] },
    { query => 'body:ennui', expected => [ 9 ] },
    { query => 'body:quinoa', expected => [ 10 ] },
    # Test that terms are matched *only* in the field requested
    { query => 'to:mixtape', expected => [ 1 ] },
    { query => 'from:mixtape', expected => [ 2 ] },
    { query => 'cc:mixtape', expected => [ 3 ] },
    { query => 'bcc:mixtape', expected => [ 4 ] },
    { query => 'subject:mixtape', expected => [ 5 ] },
    # header: matches any header
    { query => 'header:mixtape', expected => [ 1..6 ] },
    { query => 'body:mixtape', expected => [ 7 ] },
    { query => 'mixtape', expected => [ 1..7 ] },
);

sub filter_test_to_imap_search
{
    my ($t) = @_;

    my @search;
    if ($t->{query} =~ m/header:/)
    {
	# no direct equivalent
	return undef;
    }
    elsif ($t->{query} =~ m/^__begin:and/)
    {
	@search = split(/\s+/, $t->{query});
	shift(@search);
	pop(@search);
	@search = map { ('text', $_) } @search;
    }
    elsif ($t->{query} =~ m/^__begin:or/)
    {
	my @s = split(/\s+/, $t->{query});
	shift(@s);
	pop(@s);
	@search = ( 'text', shift(@s) );
	foreach my $t (@s)
	{
	    @search = ('or', 'text', $t, @search);
	}
    }
    elsif ($t->{query} =~ m/:/)
    {
	# transform 'from:foo' into 'from' 'foo'
	@search = split(/:/, $t->{query});
    }
    else
    {
	@search = ( 'text', $t->{query} );
    }
    return \@search;
}

sub prefilter_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    my $uid = 1;
    foreach my $d (@filter_data)
    {
	$exp{$uid} = $self->make_filter_message($d, $uid);
	$uid++;
    }
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    foreach my $t (@filter_tests)
    {
	xlog "Testing query \"$t->{query}\"";
	$res = run_squatter($self->{instance}, '-vv', '-e', $t->{query}, $mboxname);
	$self->assert_deep_equals({
	    $mboxname => {
		map { $_ => 1 } @{$t->{expected}}
	    }
	}, $res);
    }
}

sub rolling_test_common
{
    my ($self, $keep) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{sync_client_pid} = $self->{instance}->run_command(
		    { cyrus => 1, background => 1},
		    'squatter', '-v', '-R', '-d');

    xlog "appending a message";
    my %exp;
    $exp{1} = $self->make_message("Message A");

    xlog "check the messages got there";
    $self->check_messages(\%exp);

    # The last argument tells replication_wait() not to disconnect
    # and reconnect the client.  This means that an imapd is alive
    # and has the mailbox selected while the squatter runs.
    $self->replication_wait('squatter', $keep);

    xlog "Indexer should have indexed the message";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    1 => 1
		}
	    }
	}, $res);

    xlog "Add some more messages";
    my $N1 = 10;
    for (2..$N1)
    {
	$exp{$_} = $self->make_message("Message $_");
    }

    $self->replication_wait('squatter');
    sleep(8);

    xlog "Indexer should have indexed the new messages";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1)
		}
	    }
	}, $res);

    xlog "Sync channel directory still exists";
    my $channeldir = $self->{instance}->{basedir} .  "/conf/sync/squatter";
    $self->assert( -d $channeldir );

    xlog "No sync log files are left over";
    opendir CHANNEL, $channeldir
	or die "Cannot open $channeldir for reading: $!";
    my @files = grep { m/^log/ } readdir(CHANNEL);
    closedir CHANNEL;
    $self->assert( !scalar @files );
}

sub test_rolling_squat
    :RollingSquatter
{
    my ($self) = @_;

    xlog "test squatter rolling mode with Squat";
    $self->rolling_test_common(0);
}

sub test_rolling_sphinx
    :RollingSquatter
{
    my ($self) = @_;

    xlog "test squatter rolling mode with Sphinx";
    $self->rolling_test_common(0);
}

sub test_rolling_xapian
    :RollingSquatter
{
    my ($self) = @_;

    xlog "test squatter rolling mode with Xapian";
    $self->rolling_test_common(0);
}

sub test_rolling_sphinx_locked
    :RollingSquatter
{
    my ($self) = @_;

    xlog "Test squatter rolling mode with Sphinx and an imapd holding";
    xlog "the mboxname lock in shared mode";
    $self->rolling_test_common(1);
}

sub test_rolling_many_sphinx
    :RollingSquatter
{
    my ($self) = @_;
    $self->rolling_many_test_common();
}

sub test_rolling_many_xapian
    :RollingSquatter
{
    my ($self) = @_;
    $self->rolling_many_test_common();
}

sub rolling_many_test_common
{
    my ($self) = @_;

    xlog "test squatter rolling mode with Sphinx and many users";

    my $admintalk = $self->{adminstore}->get_client();
    my @users = ( qw(letterpress williamsburg narwhal irony
		     hoodie brooklyn gentrify seitan
		     quinoa bespoke forage selvage
		     twee cray raw denim
		     lomo) );

    xlog "creating users";
    my %uidv;
    foreach my $user (@users)
    {
	$self->{instance}->create_user($user);
	my $folder = "user." . $user;
	my $res = $admintalk->status($folder, ['uidvalidity']);
	$uidv{$folder} = $res->{uidvalidity};
    }

    $self->{sync_client_pid} = $self->{instance}->run_command(
		    { cyrus => 1, background => 1},
		    'squatter', '-v', '-R', '-d');

    xlog "appending messages";
    my $exp = {};
    foreach my $uid (1..3)
    {
	foreach my $user (@users)
	{
	    my $folder = "user.$user";
	    $self->{adminstore}->set_folder($folder);
	    my $msg = $self->make_message("Message $user $uid ",
					  store => $self->{adminstore});
	    $msg->set_attribute(uid => $uid);
	    $exp->{$folder} ||= {};
	    $exp->{$folder}->{$uid} = $msg;
	}
    }

    xlog "check the messages got there";
    foreach my $user (@users)
    {
	my $folder = "user.$user";
	xlog "folder $folder";
	$self->{adminstore}->set_folder($folder);
	$self->check_messages($exp->{$folder},
			      store => $self->{adminstore},
			      keyed_on => 'uid');
    }

    $self->replication_wait('squatter');

    xlog "Indexer should have indexed the messages";
    # Note that we have to call index_dump once for each user
    foreach my $user (@users)
    {
	my $folder = "user.$user";
	xlog "folder $folder";
	my $res = index_dump($self->{instance}, $folder);
	$self->assert_deep_equals({
		$folder => { $uidv{$folder} => { map { $_ => 1 } (1..3) } }
	}, $res);
    }
}

sub test_8bit_sphinx
{
    my ($self) = @_;

    xlog "test indexing 8bit characters with Sphinx";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{A} = $self->make_message("Message A",
				  mime_charset => 'iso-8859-1',
				  mime_encoding => '8bit',
				  # U+00E9 normalises to lowercase e
				  # U+00F4 normalises to lowercase o
				  body => "H\xe9llo W\xf4rld\r\n");
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Run the indexer";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check that the terms got indexed";
    $res = run_squatter($self->{instance}, '-vv', '-e', 'body:hello', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
    $res = run_squatter($self->{instance}, '-vv', '-e', 'body:world', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
    $res = run_squatter($self->{instance}, '-vv', '-e', 'body:quinoa', $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);
}

sub test_empty_charset_xapian
{
    my ($self) = @_;

    xlog "test indexing a message with charset=\"\" with Xapian";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{A} = $self->make_message("Message A",
				  mime_charset => '',
				  body => "Etsy quinoa\r\n");
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Run the indexer";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check that the terms got indexed";
    $res = run_squatter($self->{instance}, '-vv', '-e', 'body:etsy', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
    $res = run_squatter($self->{instance}, '-vv', '-e', 'body:quinoa', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
    $res = run_squatter($self->{instance}, '-vv', '-e', 'body:dreamcatcher', $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);
}


# This not really a test, it checks to see what the
# effecive limit on message size is when indexing to
# Sphinx.  If you're wondering, it was 8291049.
sub XXXtest_sphinx_query_limit
{
    my ($self) = @_;

    xlog "test the maximum size of an SQL query in Sphinx";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    my $lo = 1;
    my $hi = undef;
    my $uid = 1;
    my $size;

    while (!defined $hi || $hi > $lo+1)
    {
	my $mid;
	if (defined $hi)
	{
	    $mid = int(($lo + $hi)/2);
	    xlog "lo=$lo hi=$hi mid=$mid";
	}
	else
	{
	    $mid = 2*$lo;
	    xlog "lo=$lo hi=undef mid=$mid";
	}
	xlog "Append a message with $mid extra lines";

	my $msg = $self->make_message("Message $uid", extra_lines => $mid);
	xlog "Check the messages got there";
	$self->check_messages({ $uid => $msg });

	xlog "Run the indexer";
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

	my $res = sphinx_dump($self->{instance}, $mboxname);
	if (defined $res->{$mboxname}->{$uidvalidity}->{$uid})
	{
	    xlog "Successfully indexed";
	    $lo = $mid;
	    $size = length($msg);
	}
	else
	{
	    xlog "Failed to index";
	    $hi = $mid;
	}
	$talk->store($uid, '+flags', '(\\Deleted)');
	$talk->expunge();
	$uid++;
    }

    xlog "Final size is $size";
}

# make a string which uniquely represents a number
# but without being numeric, just to make sure it
# will get indexed.
sub encode_number
{
    my ($n) = @_;
    my @digits = ('alpha', 'beta', 'gamma', 'delta', 'epsilon',
		  'zeta', 'eta', 'theta', 'iota', 'kappa');
    my $s = '';

    return $digits[0] if !$n;

    while ($n) {
	$s = $digits[$n % 10] . "$s";
	$n = int($n / 10);
    }
    return $s;
}

sub test_sphinx_large_query
{
    my ($self) = @_;
    xlog "test truncation of large messages with Sphinx";
    $self->large_query_test_common();
}

sub test_xapian_large_query
{
    my ($self) = @_;
    xlog "test truncation of large messages with Xapian";
    $self->large_query_test_common();
}

sub large_query_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "Append a message which is large but below the limit";
    my %exp;
    my $body = '';
    my $n = 0;
    while (length($body) < 3*1024*1024)
    {
	$body .= encode_number($n) . "\r\n";
	$n++;
    }
    my $untruncated_n = $n-1;
    $exp{1} = $self->make_message("Message 1", body => $body);

    xlog "Append a message which is definitely over the limit";
    while (length($body) < 6*1024*1024)
    {
	$body .= encode_number($n) . "\r\n";
	$n++;
    }
    my $truncated_n = $n-1;
    $exp{2} = $self->make_message("Message 2", body => $body);

    xlog "Check the messages got there";
    $self->check_messages(\%exp);

    xlog "Run the indexer";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check that both messages were indexed";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    1 => 1,
		    2 => 1,
		}
	    }
	}, $res);

    xlog "Check that the untruncated text is found for both messages";
    $res = run_squatter($self->{instance}, '-vv', '-e',
		      'body:' .  encode_number($untruncated_n), $mboxname);
    $self->assert_deep_equals({
	$mboxname => {
	    1 => 1,
	    2 => 1
	}
    }, $res);

    xlog "Check that the truncated text is not found";
    $res = run_squatter($self->{instance}, '-vv', '-e',
		      'body:' .  encode_number($truncated_n), $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);
}

sub test_sphinx_prefilter_multi
{
    my ($self) = @_;

    xlog "test squatter with multiple folders and Sphinx";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    my @folders = ( 'kale', 'tofu', 'smallbatch' );

    xlog "create folders";
    foreach my $folder (@folders)
    {
	$talk->create("$mboxname.$folder")
	    or die "Cannot create folder $mboxname.$folder: $@";
    }

    xlog "append some messages";
    my $exp = {};
    map { $exp->{$_} = {}; } @folders;
    my $uid = 1;
    my $folderidx = 0;
    foreach my $d (@filter_data)
    {
	my $folder = $folders[$folderidx];
	$self->{store}->set_folder("$mboxname.$folder");
	$exp->{$folder}->{$uid} = $self->make_filter_message($d, $uid);

	$folderidx++;
	if ($folderidx >= scalar(@folders)) {
	    $folderidx = 0;
	    $uid++;
	}
    }

    xlog "check the messages got there";
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder("$mboxname.$folder");
	$self->check_messages($exp->{$folder});
    }

    xlog "Index the messages";
    foreach my $folder (@folders)
    {
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$mboxname.$folder");
    }

    xlog "Check the results of the index run";
    foreach my $t (@filter_tests)
    {
	xlog "Testing query \"$t->{query}\"";
	$res = run_squatter($self->{instance}, '-vvm', '-e', $t->{query}, $mboxname);

	my $exp = {};
	foreach my $i (@{$t->{expected}})
	{
	    my $folder = $mboxname . "." . $folders[($i-1) % scalar(@folders)];
	    my $uid = int(($i-1) / scalar(@folders)) + 1;
	    $exp->{$folder} ||= {};
	    $exp->{$folder}->{$uid} = 1;
	}
	xlog "expecting " . Data::Dumper::Dumper($exp);

	$self->assert_deep_equals($exp, $res);
    }
}

sub test_sphinx_xconvmultisort
{
    my ($self) = @_;

    xlog "test the XCONVMULTISORT command";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res;
    my %uidvalidity;
    my $hms = 0;

    my @folders = ( 'kale', 'smallbatch', 'tofu' );

    xlog "create folders";
    foreach my $folder (@folders)
    {
	my $ff = "$mboxname_ext.$folder";
	$talk->create($ff)
	    or die "Cannot create folder $ff: $@";
	$res = $talk->status($ff, ['uidvalidity', 'highestmodseq']);
	$uidvalidity{$ff} = $res->{uidvalidity};
	$hms = $res->{highestmodseq} if ($hms < $res->{highestmodseq});
    }

    xlog "append some messages";
    my $exp = {};
    map { $exp->{$_} = {}; } @folders;
    my $uid = 1;
    my $folderidx = 0;
    foreach my $d (@filter_data)
    {
	my $folder = $folders[$folderidx];
	$self->{store}->set_folder("$mboxname_ext.$folder");
	$exp->{$folder}->{$uid} = $self->make_filter_message($d, $uid);

	$folderidx++;
	if ($folderidx >= scalar(@folders)) {
	    $folderidx = 0;
	    $uid++;
	}
	$hms++;
    }

    xlog "check the messages got there";
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder("$mboxname_ext.$folder");
	$self->check_messages($exp->{$folder});
    }

    xlog "Index the messages";
    foreach my $folder (@folders)
    {
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$mboxname_int.$folder");
    }

    xlog "Check the results of the index run";
    foreach my $t (@filter_tests)
    {
	xlog "Testing query \"$t->{query}\"";

	my $search = filter_test_to_imap_search($t);
	next if !defined $search;

	$res = $self->{store}->xconvmultisort(sort => [ 'uid', 'folder' ],
					      search => [ @$search ]);
	xlog "res = " . Dumper($res);

	my $exp = {
	    highestmodseq => $hms,
	    total => scalar(@{$t->{expected}}),
	    position => 1,
	    uidvalidity => {},
	    xconvmulti => []
	};
	if (!scalar @{$t->{expected}})
	{
	    delete $exp->{position};
	    delete $exp->{xconvmulti};
	    delete $exp->{uidvalidity};
	    $exp->{total} = 0;
	}
	foreach my $i (@{$t->{expected}})
	{
	    my $folder = "$mboxname_ext." . $folders[($i-1) % scalar(@folders)];
	    my $uid = int(($i-1) / scalar(@folders)) + 1;
	    push(@{$exp->{xconvmulti}}, [ $folder, $uid ]);
	    $exp->{uidvalidity}->{$folder} = $uidvalidity{$folder};
	}
	xlog "expecting " . Data::Dumper::Dumper($exp);

	$self->assert_deep_equals($exp, $res);
    }
}

sub test_sphinx_xconvmultisort_anchor
{
    my ($self) = @_;

    xlog "test the XCONVMULTISORT command with an ANCHOR";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res;
    my %uidvalidity;

    my @folders = ( 'kale', 'tofu', 'smallbatch' );

    xlog "create folders";
    foreach my $folder (@folders)
    {
	my $ff = "$mboxname_ext.$folder";
	$talk->create($ff)
	    or die "Cannot create folder $ff: $@";
	$res = $talk->status($ff, ['uidvalidity']);
	$uidvalidity{$ff} = $res->{uidvalidity};
    }

    my @subjects = ( qw(
	gastropub hella mumblecore brooklyn locavore fingerstache
	twee semiotics VHS Austin stumptown vinyl irony organic
	Helvetica vice cliche tumblr dreamcatcher
    ) );
    # lexically sorted order of indexes into @subjects
    my @order = ( 9, 3, 16, 18, 5, 0, 1, 14, 12, 4,
		  2, 13, 7, 10, 17, 6, 8, 15, 11 );
    my @sorted_tuples;
    map
    {
	my $i = 0 + $_;
	push(@sorted_tuples, [
	    "$mboxname_ext." . $folders[$i % scalar(@folders)],
	    int($i / scalar(@folders)) + 1
	]);
    } @order;

    xlog "append some messages";
    my $exp = {};
    map { $exp->{$_} = {}; } @folders;
    my $uid = 1;
    my $folderidx = 0;
    foreach my $s (@subjects)
    {
	my $folder = $folders[$folderidx];
	$self->{store}->set_folder("$mboxname_ext.$folder");
	my $msg = $self->make_message($s);
	$exp->{$folder}->{$uid} = $msg;
	$msg->set_attribute(uid => $uid);

	$folderidx++;
	if ($folderidx >= scalar(@folders)) {
	    $folderidx = 0;
	    $uid++;
	}
    }

    xlog "check the messages got there";
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder("$mboxname_ext.$folder");
	$self->check_messages($exp->{$folder});
    }

    xlog "Index the messages";
    foreach my $folder (@folders)
    {
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$mboxname_int.$folder");
    }

    xlog "Check the results of the index run";
    $folderidx = 0;
    $uid = 1;
    foreach my $s (@subjects)
    {
	xlog "Testing subject \"$s\"";

	$res = $self->{store}->xconvmultisort(search => [ 'subject', "\"$s\"" ]);
	delete $res->{highestmodseq} if defined $res;
	xlog "res = " . Dumper($res);

	my $folder = "$mboxname_ext." . $folders[$folderidx];
	my $exp = {
	    total => 1,
	    position => 1,
	    xconvmulti => [],
	    uidvalidity => { $folder => $uidvalidity{$folder} }
	};
	push(@{$exp->{xconvmulti}}, [ $folder, $uid ]);

	$folderidx++;
	if ($folderidx >= scalar(@folders)) {
	    $folderidx = 0;
	    $uid++;
	}
	xlog "expecting " . Data::Dumper::Dumper($exp);

	$self->assert_deep_equals($exp, $res);
    }

    xlog "Check that cross-folder sorting works";
    $res = $self->{store}->xconvmultisort(sort => [ 'subject' ]);
    delete $res->{highestmodseq} if defined $res;
    my $exp2 = {
	total => scalar(@sorted_tuples),
	position => 1,
	xconvmulti => \@sorted_tuples,
	uidvalidity => \%uidvalidity
    };
    $self->assert_deep_equals($exp2, $res);

    xlog "Check that MULTIANCHOR works";
    for (my $i = 0 ; $i < scalar(@subjects) ; $i++)
    {
	my $folder = $sorted_tuples[$i]->[0];
	my $uid = $sorted_tuples[$i]->[1];
	xlog "$i: folder $folder uid $uid";
	$res = $self->{store}->xconvmultisort(
	    sort => [ 'subject' ],
	    windowargs => [
		'MULTIANCHOR', [ $uid, $folder, 0, 1 ]
	    ]
	);
	delete $res->{highestmodseq} if defined $res;
	$self->assert_deep_equals({
	    total => scalar(@sorted_tuples),
	    position => ($i+1),
	    xconvmulti => [ [ $folder, $uid ] ],
	    uidvalidity => \%uidvalidity
	}, $res);
    }
}

sub test_sphinx_iris1936
    :SmallBatchsize
{
    my ($self) = @_;

    xlog "Regression test for IRIS-1936, where squatter with Sphinx";
    xlog "would loop forever multiply indexing the first 20 messages";
    xlog "in a mailbox which was the 21st mailbox with the same";
    xlog "uidvalidity";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    xlog "create some folders";
    my $lastuidv;
    my @folders;
    my $n = 0;
    while (scalar @folders < 21)
    {
	my $folder = $mboxname . '.' . encode_number($n);
	$talk->create($folder)
	    or die "Cannot create folder $folder: $@";
	my $res = $talk->status($folder, ['uidvalidity']);
	my $uidv = $res->{uidvalidity};
	if (!defined $lastuidv || $lastuidv == $uidv)
	{
	    push(@folders, $folder);
	}
	else
	{
	    @folders = ();
	}
	$lastuidv = $uidv;

	# avoid looping forever if creation is too slow
	$n++;
	die "Cannot create folders fast enough to setup test"
	    if ($n > 50);
    }

    xlog "Usable folders: " . join(' ', @folders);

    xlog "Append one message to each folder";
    my %exp;
    my $res;
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder($folder);
	$self->{gen}->set_next_uid(1);
	$exp{$folder} ||= {};
	$exp{$folder}->{1} = $self->make_message("Message 1");
    }

    xlog "Append more than one batch's worth to the 21st folder";
    my $folder = $folders[20];
    $self->{store}->set_folder($folder);
    $self->{gen}->set_next_uid(2);
    foreach my $uid (2..6)
    {
	$exp{$folder}->{$uid} = $self->make_message("Message $uid");
    }

    xlog "Check the messages got there";
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder($folder);
	$self->check_messages($exp{$folder});
    }

    xlog "Index run on first 20 folders";
    foreach (0..19)
    {
	$folder = $folders[$_];
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $folder);
    }

    xlog "Index run on 21st folder; will loop if the bug is present";
    $folder = $folders[20];
    my $pid = $self->{instance}->run_command(
				{ cyrus => 1, background => 1 },
			       'squatter', '-ivv', $folder);
    eval
    {
	timed_wait(sub {
	    # nonblock waitpid()
	    my $r = $self->{instance}->reap_command($pid, 1);
	    defined $r && $r == 0;
	}, description => "squatter to finish indexing $folder");
    };
    my $ex = $@;
    if ($ex)
    {
	xlog "Timed out, the test has FAILED";
	$self->{instance}->stop_command($pid);
	die $ex;
    }
    xlog "Finished normally, yay";

    xlog "Check the results of the index runs";
    my $iexp = {};
    foreach (0..19)
    {
	$folder = $folders[$_];
	$iexp->{$folder} = { $lastuidv => { 1 => 1 } };
    }
    $folder = $folders[20];
    $iexp->{$folder} = { $lastuidv => { map { $_ => 1 } (1..6) } };
    $self->assert_deep_equals($iexp, index_dump($self->{instance}));
}

sub test_sphinx_null_multipart
{
    my ($self) = @_;

    xlog "Regression test for one of the bugs in IRIS-1912; reading the";
    xlog "BODYSTRUCTURE cache for a message with content-type=multipart/whatever";
    xlog "but no parts at all, which is illegal according to RFC2046 but";
    xlog "has been seen in the wild";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  mime_type => 'multipart/alternative',
				  body => '');
    xlog "check the message got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => { $uidvalidity => { 1 => 1 } } }, $res);
}

sub test_sphinx_trivial_multipart
{
    my ($self) = @_;

    xlog "Regression test for one of the bugs in IRIS-1912; reading the";
    xlog "BODYSTRUCTURE cache for a message with content-type=multipart/whatever";
    xlog "and some body text but no parts boundaries, which is illegal";
    xlog "according to RFC2046 but has been seen in the wild";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  mime_type => 'multipart/mixed',
				  body => "Dreamcatcher brooklyn\r\n");
    xlog "check the message got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => { $uidvalidity => { 1 => 1 } } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'dreamcatcher', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'brooklyn', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
}

sub test_sphinx_single_multipart
{
    my ($self) = @_;

    xlog "Regression test for one of the bugs in IRIS-1912; reading the";
    xlog "BODYSTRUCTURE cache for a message with content-type=multipart/whatever";
    xlog "and some body text but no parts boundaries, which is illegal";
    xlog "according to RFC2046 but has been seen in the wild";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  mime_type => 'multipart/mixed',
				  mime_boundary => 'COSBY-SWEATER',
				  body =>
				    "--COSBY-SWEATER\r\n" .
				    "\r\n" .
				    "Quinoa etsy\r\n" .
				    "--COSBY-SWEATER--\r\n");
    xlog "check the message got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivvvv', $mboxname);

    xlog "Check the results of the index run";
    $res = sphinx_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => { $uidvalidity => { 1 => 1 } } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'quinoa', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'etsy', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
}

sub test_sphinx_null_text
{
    my ($self) = @_;

    xlog "Regression test for one of the bugs in IRIS-1912; reading the";
    xlog "SECTIONS cache for a message with content-type=text/plain";
    xlog "but no body at all";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  mime_type => 'text/plain',
				  body => '');
    xlog "check the message got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = sphinx_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => { $uidvalidity => { 1 => 1 } } }, $res);
}

sub test_sphinx_unindexed_sort_all
{
    my ($self) = @_;

    xlog "test that SORT...ALL works in the presence of";
    xlog "unindexed messages, under Sphinx";
    $self->unindexed_sort_all_common();
}

sub test_squat_unindexed_sort_all
{
    my ($self) = @_;

    xlog "test that SORT...ALL works in the presence of";
    xlog "unindexed messages, under SQUAT";
    $self->unindexed_sort_all_common();
}

sub test_xapian_unindexed_sort_all
{
    my ($self) = @_;

    xlog "test that SORT...ALL works in the presence of";
    xlog "unindexed messages, under Xapian";
    $self->unindexed_sort_all_common();
}

sub unindexed_sort_all_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    xlog "append 3 messages";
    my %exp;
    $exp{A} = $self->make_message('Message A');
    $exp{B} = $self->make_message('Message B');
    $exp{C} = $self->make_message('Message C');

    my $res;
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "SORT works";
    $res = $talk->sort(['uid'], 'utf-8', 'all');
    $self->assert_deep_equals([ 1, 2, 3 ], $res);

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "SORT still works";
    $res = $talk->sort(['uid'], 'utf-8', 'all');
    $self->assert_deep_equals([ 1, 2, 3 ], $res);

    xlog "Append another message";
    $exp{D} = $self->make_message('Message D');

    xlog "SORT sees the new, unindexed, message";
    $res = $talk->sort(['uid'], 'utf-8', 'all');
    $self->assert_deep_equals([ 1, 2, 3, 4 ], $res);

    xlog "Index the new message";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "SORT still sees the new, now indexed, message";
    $res = $talk->sort(['uid'], 'utf-8', 'all');
    $self->assert_deep_equals([ 1, 2, 3, 4 ], $res);
}

sub test_sphinx_unindexed_sort_subject
{
    my ($self) = @_;

    xlog "test that SORT...SUBJECT works in the presence of";
    xlog "unindexed messages, under Sphinx";
    $self->unindexed_sort_subject_common();
}

sub test_squat_unindexed_sort_subject
{
    my ($self) = @_;

    xlog "test that SORT...SUBJECT works in the presence of";
    xlog "unindexed messages, under SQUAT";
    $self->unindexed_sort_subject_common();
}

sub test_xapian_unindexed_sort_subject
{
    my ($self) = @_;

    xlog "test that SORT...SUBJECT works in the presence of";
    xlog "unindexed messages, under Xapian";
    $self->unindexed_sort_subject_common();
}

sub unindexed_sort_subject_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    xlog "append 3 messages";
    my %exp;
    $exp{A} = $self->make_message('Vinyl');
    $exp{B} = $self->make_message('Tattooed');
    $exp{C} = $self->make_message('Portland');

    my $res;
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "SORT...SUBJECT works";
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'vinyl');
    $self->assert_deep_equals([ 1 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'tattooed');
    $self->assert_deep_equals([ 2 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'portland');
    $self->assert_deep_equals([ 3 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'brooklyn');
    $self->assert_deep_equals([ ], $res);

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "SORT still works";
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'vinyl');
    $self->assert_deep_equals([ 1 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'tattooed');
    $self->assert_deep_equals([ 2 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'portland');
    $self->assert_deep_equals([ 3 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'brooklyn');
    $self->assert_deep_equals([ ], $res);

    xlog "Append another message";
    $exp{D} = $self->make_message('Brooklyn');

    xlog "SORT sees the new, unindexed, message";
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'vinyl');
    $self->assert_deep_equals([ 1 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'tattooed');
    $self->assert_deep_equals([ 2 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'portland');
    $self->assert_deep_equals([ 3 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'brooklyn');
    $self->assert_deep_equals([ 4 ], $res);

    xlog "Index the new message";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "SORT still sees the new, now indexed, message";
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'vinyl');
    $self->assert_deep_equals([ 1 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'tattooed');
    $self->assert_deep_equals([ 2 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'portland');
    $self->assert_deep_equals([ 3 ], $res);
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'brooklyn');
    $self->assert_deep_equals([ 4 ], $res);
}

sub test_squatter_whitespace_squat
{
    my ($self) = @_;
    xlog "test squatter on a folder name with a space in it, with SQUAT";
    $self->squatter_whitespace_test_common();
}

sub test_squatter_whitespace_sphinx
{
    my ($self) = @_;
    xlog "test squatter on a folder name with a space in it, with Sphinx";
    $self->squatter_whitespace_test_common();
}

sub test_squatter_whitespace_xapian
{
    my ($self) = @_;
    xlog "test squatter on a folder name with a space in it, with Xapian";
    $self->squatter_whitespace_test_common();
}

sub squatter_whitespace_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane.Etsy Quinoa';
    $talk->create($mboxname) || die "Cannot create folder $mboxname: $@";
    $self->{store}->set_folder($mboxname);

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    my $N1 = 10;
    for (1..$N1)
    {
	$exp{$_} = $self->make_message("Message $_");
    }
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Before first index, there is nothing to dump";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({}, $res);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1)
		}
	    }
	}, $res);
}

sub test_squatter_domain_squat
    :VirtualDomains
{
    my ($self) = @_;
    xlog "test squatter on a folder name in a domain, with SQUAT";
    $self->squatter_domain_test_common();
}

sub test_squatter_domain_sphinx
    :VirtualDomains
{
    my ($self) = @_;
    xlog "test squatter on a folder name in a domain, with Sphinx";
    $self->squatter_domain_test_common();
}

sub test_squatter_domain_xapian
    :VirtualDomains
{
    my ($self) = @_;
    xlog "test squatter on a folder name in a domain, with Xapian";
    $self->squatter_domain_test_common();
}

sub squatter_domain_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $admintalk = $self->{adminstore}->get_client();
    my @users = ( 'cosby@sweater.com', 'jean@shorts.org' );
    my @mbs;
    my %uidvalidity;
    my $res;

    map { push(@mbs, Cassandane::Mboxname->new(
		config => $self->{instance}->{config},
		username => $_)); } @users;

    xlog "Create users";
    foreach my $mb (@mbs)
    {
	$self->{instance}->create_user($mb->to_username);
	$res = $admintalk->status($mb->to_external, ['uidvalidity']);
	$uidvalidity{$mb} = $res->{uidvalidity};
    }

    xlog "Append some messages";
    my %exp;
    my $N1 = 10;
    for (1..$N1)
    {
	my $mb = $mbs[($_-1) % scalar(@mbs)];
	my $uid = int(($_-1) / scalar(@mbs))+1;
	$self->{adminstore}->set_folder($mb->to_external);
	my $msg = $self->make_message("Message $uid", store => $self->{adminstore});
	$msg->set_attribute(uid => $uid);
	$exp{$mb}->{$_} = $msg;
    }

    xlog "check the messages got there";
    foreach my $mb (@mbs)
    {
	$self->{adminstore}->set_folder($mb->to_external);
	$self->check_messages($exp{$mb}, store => $self->{adminstore});
    }

    xlog "Before first index, there is nothing to dump";
    foreach my $mb (@mbs)
    {
	$res = index_dump($self->{instance}, $mb);
	$self->assert_deep_equals({}, $res);
    }

    xlog "Index run";
    foreach my $mb (@mbs)
    {
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$mb");
    }

    xlog "Check the results of the index run";
    foreach my $mb (@mbs)
    {
	$res = index_dump($self->{instance}, $mb);
	$self->assert_deep_equals({
		"$mb" => {
		    $uidvalidity{$mb} => {
			map { $_ => 1 } (1..scalar(keys %{$exp{$mb}}))
		    }
		}
	    }, $res);
    }
}

sub test_sphinx_query_limit
    :Xconvmultisort
{
    my ($self) = @_;

    xlog "test that Sphinx' default LIMIT 20 on queries is defeated";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';
    $self->{store}->set_folder($mboxname);

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    my $N1 = 30;
    for (1..$N1)
    {
	my $s = "Quinoa x" . sprintf("%07d", $_);
	$exp{$_} = $self->make_message($s);
    }
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = sphinx_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1)
		}
	    }
	}, $res);

    xlog "Check that SORT can see all the messages";
    $res = $talk->sort(['uid'], 'utf-8', 'subject', 'quinoa');
    $self->assert_deep_equals([ 1..$N1 ], $res);

    xlog "Check that XCONVMULTISORT can see all the messages";
    $res = $self->{store}->xconvmultisort(
		    sort => ['uid'],
		    search => ['subject', 'quinoa']
    );
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	position => 1,
	total => $N1,
	xconvmulti => [ map { [ "INBOX", $_ ] } (1..$N1) ],
	uidvalidity => { "INBOX" => $uidvalidity },
    }, $res);

}

sub xstats_delta
{
    my ($before, $after) = @_;
    my $res = {};

    foreach my $m (sort { $a cmp $b } keys %$after)
    {
	$res->{$m} = $after->{$m} - $before->{$m};
	xlog "xstat $m " . $res->{$m} if get_verbose;
    }
    return $res;
}

# This test relies on statistics emitted from the internals
# of the search implementation, which has changed since the
# test was written.  It simply won't work at present, so we
# disable it.
sub XXXtest_sphinx_xconvmultisort_optimisation
{
    my ($self) = @_;

    xlog "test performance optimisations of the XCONVMULTISORT command";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res;
    my %uidvalidity;

    my @folders = ( 'kale', 'smallbatch', 'tofu' );

    xlog "create folders";
    foreach my $folder (@folders)
    {
	my $ff = "$mboxname_ext.$folder";
	$talk->create($ff)
	    or die "Cannot create folder $ff: $@";
	$res = $talk->status($ff, ['uidvalidity']);
	$uidvalidity{$ff} = $res->{uidvalidity};
    }

    xlog "append some messages";
    my $exp = {};
    map { $exp->{$_} = {}; } @folders;
    my $uid = 1;
    my $hms = 6;
    my $folderidx = 0;
    foreach my $d (@filter_data)
    {
	my $folder = $folders[$folderidx];
	$self->{store}->set_folder("$mboxname_ext.$folder");
	$exp->{$folder}->{$uid} = $self->make_filter_message($d, $uid);

	$folderidx++;
	if ($folderidx >= scalar(@folders)) {
	    $folderidx = 0;
	    $uid++;
	}
	$hms++;
    }

    xlog "check the messages got there";
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder("$mboxname_ext.$folder");
	$self->check_messages($exp->{$folder});
    }

    xlog "Index the messages";
    foreach my $folder (@folders)
    {
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$mboxname_int.$folder");
    }

    my @tests = (
	{
	    # Querying an indexed header like TO/FROM/CC/BCC/SUBJECT
	    query => [ 'to', 'mixtape' ],
	    expected => {
		MESSAGE_MAP => 0,
		MSGDATA_LOAD => 1,
		SEARCH_BODY => 0,
		SEARCH_CACHE_HEADER => 0,
		SEARCH_EVALUATE => 1,
		SEARCH_HEADER => 0,
		SEARCH_RESULT => 1,
		SEARCH_TRIVIAL => 0,
		SPHINX_MATCH => 1,
		SPHINX_MULTIPLE => 1,
		SPHINX_QUERY => 1,
		SPHINX_RESULT => 1,
		SPHINX_ROW => 1,
		SPHINX_SINGLE => 0,
		SPHINX_UNINDEXED => 0,
	    }
	},
	{
	    # Querying an unindexed cached header
	    query => [ 'header', 'narwhal', 'mixtape' ],
	    expected => {
		MESSAGE_MAP => 0,
		MSGDATA_LOAD => 6,
		SEARCH_BODY => 0,
		SEARCH_CACHE_HEADER => 6,
		SEARCH_EVALUATE => 6,
		SEARCH_HEADER => 0,
		SEARCH_RESULT => 1,
		SEARCH_TRIVIAL => 0,
		SPHINX_MATCH => 2,
		SPHINX_MULTIPLE => 1,
		SPHINX_QUERY => 1,
		SPHINX_RESULT => 6,
		SPHINX_ROW => 6,
		SPHINX_SINGLE => 0,
		SPHINX_UNINDEXED => 0,
	    }
	},
	{
	    # Querying the body
	    query => [ 'body', 'mixtape' ],
	    expected => {
		MESSAGE_MAP => 0,
		MSGDATA_LOAD => 1,
		SEARCH_BODY => 0,
		SEARCH_CACHE_HEADER => 0,
		SEARCH_EVALUATE => 1,
		SEARCH_HEADER => 0,
		SEARCH_RESULT => 1,
		SEARCH_TRIVIAL => 0,
		SPHINX_MATCH => 1,
		SPHINX_MULTIPLE => 1,
		SPHINX_QUERY => 1,
		SPHINX_RESULT => 1,
		SPHINX_ROW => 1,
		SPHINX_SINGLE => 0,
		SPHINX_UNINDEXED => 0,
	    }
	},
    );

    foreach my $t (@tests)
    {
	xlog "Check side effects of: " . join(' ', @{$t->{query}});
	my $before = $self->{store}->xstats();
	$res = $self->{store}->xconvmultisort(search => [ @{$t->{query}} ])
	    or die "XCONVMULTISORT failed: $@";
	my $delta = xstats_delta($before, $self->{store}->xstats());
	my $xdelta = { map { $_ => $delta->{$_} } keys %{$t->{expected}} };
	$self->assert_deep_equals($xdelta, $t->{expected});
    }
}

sub test_sphinx_xconvmultisort_metachar
{
    my ($self) = @_;

    xlog "test quoting of SphinxQL metacharacters in the XCONVMULTISORT command";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname_ext, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    my @subjects = (
	"thund!ercats", "card\"igan", "ho\$odie",
	"sar'torial", "gen-trify", "ve/gan",
	"semi<otics", "bl=og", "vin\@yl",
	"br[unch", "ir\\ony", "um]mi",
	"org^anic", "pit|chfork", "iph~one"
    );

    xlog "Append some messages";
    my %exp;
    my $uid = 1;
    foreach my $s (@subjects)
    {
	$exp{$uid} = $self->make_message('narwhal ' . $s . ' whatever');
	$uid++;
    }

    xlog "check the messages got there";
    $self->check_messages(\%exp, keyed_on => 'uid');

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname_int);

    xlog "Search for \"narwhal\", all messages have it";
    $res = $self->{store}->xconvmultisort(search => [ 'subject', { Quote => 'narwhal' } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    my $N = scalar(@subjects);
    $self->assert_deep_equals({
	total => $N,
	position => 1,
	xconvmulti => [ map { [ $mboxname_ext, $_ ] } (1..$N) ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);

    xlog "Search for \"whatever\", all messages have it";
    $res = $self->{store}->xconvmultisort(search => [ 'subject', { Quote => 'whatever' } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => $N,
	position => 1,
	xconvmulti => [ map { [ $mboxname_ext, $_ ] } (1..$N) ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);

    xlog "Search for each subject in turn";
    $uid = 1;
    foreach my $s (@subjects)
    {
	xlog "Search for subject $s";
	$res = $self->{store}->xconvmultisort(search => [ 'subject', { Quote => $s } ])
	    or die "XCONVMULTISORT failed: $@";
	delete $res->{highestmodseq} if defined $res;
	$self->assert_deep_equals({
	    total => 1,
	    position => 1,
	    xconvmulti => [ [ $mboxname_ext, $uid ] ],
	    uidvalidity => { $mboxname_ext => $uidvalidity }
	}, $res);
	$uid++;
    }
}

# Note, Squat does not support multiple folder searching

sub test_sphinx_xconvmultisort_unindexed_flags
{
    my ($self) = @_;
    xlog "test the XCONVMULTISORT command with unindexed messages";
    xlog "and searches which don't use the search engine [IRIS-2011]";
    $self->xconvmultisort_unindexed_flags_test_common();
}

sub test_xapian_xconvmultisort_unindexed_flags
{
    my ($self) = @_;
    xlog "test the XCONVMULTISORT command with unindexed messages";
    xlog "and searches which don't use the search engine [IRIS-2011]";
    $self->xconvmultisort_unindexed_flags_test_common();
}

sub xconvmultisort_unindexed_flags_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname_ext, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "Append two messages";
    my %exp;
    $exp{A} = $self->make_message('artparty');	    # UID 1
    $exp{B} = $self->make_message('brooklyn');	    # UID 2

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname_int);

    xlog "Append two more messages, these will not be indexed";
    $exp{C} = $self->make_message('cosby');	    # UID 3
    $exp{D} = $self->make_message('dreamcatcher');  # UID 4

    xlog "Check the messages got there";
    $self->check_messages(\%exp);

    $talk->store(1, '+flags', [ '\Draft' ]);
    $talk->store(3, '+flags', [ '\Draft' ]);

    xlog "Search for DRAFT";
    $res = $self->{store}->xconvmultisort(search => [ 'draft' ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 2,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 1 ], [ $mboxname_ext, 3 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);

    xlog "Search for UNDRAFT";
    $res = $self->{store}->xconvmultisort(search => [ 'undraft' ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 2,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 2 ], [ $mboxname_ext, 4 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);
}

# Sphinx is currently broken with cuneiform characters, I suspect
# because they're off the BMP.  But nobody really cares.
sub XXXtest_sphinx_xconvmultisort_sumerian
{
    my ($self) = @_;

    xlog "test the XCONVMULTISORT command Sumerian characters [IRIS-2007]";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname_ext, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "Append messages";
    my %exp;
    # These are the three lines of an ancient Sumerian proverb which translates as
    #
    #	A disorderly son -
    #	His mother should not have given birth to him,
    #	His god should not have created him.
    #
    $exp{A} = $self->make_message('Message A',
				  mime_charset => 'utf-8',
				  mime_encoding => 'quoted-printable',
				  body => "=F0=92=8C=89=F0=92=8B=9B=\r\n"
					  . "=F0=92=89=A1=F0=92=81=B2\r\n"
					  . "dumu si nu-sa2\r\n");
    $exp{B} = $self->make_message('Message B',
				  mime_charset => 'utf-8',
				  mime_encoding => 'quoted-printable',
				  body =>
					  "=F0=92=82=BC=F0=92=80=80=F0=92=89=\r\n"
					  . "=8C=F0=92=88=BE=F0=92=80=AD=F0=92=\r\n"
					  . "=85=86=F0=92=81=B3=F0=92=8C=85\r\n"
					  . "ama-a-ni na-an-u3-(dib?)-tud\r\n");
    $exp{C} = $self->make_message('Message C',
				  mime_charset => 'utf-8',
				  mime_encoding => 'quoted-printable',
				  body => "=F0=92=80=AD=F0=92=8A=8F=F0=92=88=\r\n"
					  .  "=BE=F0=92=80=AD=F0=92=81=B6=F0=\r\n"
					  . "=92=81=B6=F0=92=82=8A\r\n"
					  . "dig^ir-ra-ni na-an-dim2-dim2-e\r\n");

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivvvv', $mboxname_int);

    xlog "Check the messages got there";
    $self->check_messages(\%exp);

    xlog "Search for U+12309 CUNEIFORM SIGN TUR(DUMU)";
    $res = $self->{store}->xconvmultisort(search => [ 'subject', { Literal => "\xf0\x92\x8c\x89" } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 1,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 1 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);
}

# Note, Squat does not support multiple folder searching

sub test_sphinx_xconvmultisort_cjk
{
    my ($self) = @_;

    xlog "Test the XCONVMULTISORT command with Chinese/Japanese/Korean";
    xlog "characters [IRIS-2007]";
    $self->xconvmultisort_cjk_test_common();
}

sub test_xapian_xconvmultisort_cjk
{
    my ($self) = @_;

    xlog "Test the XCONVMULTISORT command with Chinese/Japanese/Korean";
    xlog "characters [IRIS-2007]";
    $self->xconvmultisort_cjk_test_common();
}

sub xconvmultisort_cjk_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname_ext, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "Append messages";
    my %exp;
    # Chinese lorem ipsum from http://generator.lorem-ipsum.info/
    # I have no idea what it means.
    $exp{A} = $self->make_message('Chinese Message',
				  mime_charset => 'utf-8',
				  mime_encoding => 'quoted-printable',
				  body => "=E7=B7=B7=E7=B7=A6 =\r\n"
					  . "=E6=A8=9B=E6=A7=B7=E6=AE=\r\n"
					  . "=A6=E6=A6=93=E7=94=82=E7=\r\n"
					  . "=9D=AE =E9=AB=AC =E8=AD=BA=\r\n"
					  . "=E9=90=BC=E9=9C=BA =\r\n"
					  . "=E5=B5=89=E6=84=8A=E6=83=B5 =\r\n"
					  . "=E7=86=A4=E7=86=A1=E7=A3=8E =\r\n"
					  . "=E6=BC=BB=E6=BC=8D =E8=A6=9F\r\n");
    # Japanese lorem ipsum from http://generator.lorem-ipsum.info/
    # I have no idea what it means.
    $exp{B} = $self->make_message('Japanese Message',
				  mime_charset => 'utf-8',
				  mime_encoding => 'quoted-printable',
				  body => "=E5=B6=A3=E3=81=91=E4=A6=A6=E3=81=B2==\r\n"
					  . "E6=A7=8E =E3=81=AD=E8=B6=A3=E3=83=92="
					  . "=E3=82=A7=E8=AA=A8=E4=A4=A9 =E3=83=92=\r\n"
					  . "=E3=83=A5=E9=AA=A3=E4=B0=A9=E3=81=93=\r\n"
					  . "=E3=81=88 =E8=AB=A7=E3=81=BF=E3=82=85 \r\n"
					  . "=E3=82=AD=E3=83=A5=E6=A6=9F=E4=A7=A5,=\r\n"
					  . " =E3=83=AA=E3=81=8E=E3=82=87\r\n");
    # Korean lorem ipsum from http://forums.adobe.com/thread/793576
    # I have no idea what it means
    $exp{C} = $self->make_message('Korean Message',
				  mime_charset => 'utf-8',
				  mime_encoding => 'quoted-printable',
				  body => "=EC=82=AC=EC=9A=A9=ED=95=A0 =\r\n"
					  . "=EC=88=98=EC=9E=88=EB=8A=94 =\r\n"
					  . "=EA=B5=AC=EC=A0=88 =\r\n"
					  . "=EB=A7=8E=EC=9D=80 =EB=B3=80=ED=\r\n"
					  . "=99=94=EA=B0=80 =EC=9E=88=EC=A7=\r\n"
					  . "=80=EB=A7=8C, =EB=8C=80=EB=B6=80=\r\n"
					  . "=EB=B6=84=EC=9D=98, =EC=A3=BC\r\n");
    $exp{D} = $self->make_message('=?utf-8?q?Chinese=E8=AF=B6Subject?=');

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivvvv', $mboxname_int);

    xlog "Check the messages got there";
    $self->check_messages(\%exp);

    xlog "Search for U+7DF7 which doesn't seem to have a name";
    $res = $self->{store}->xconvmultisort(search => [ 'body', { Literal => "\xe7\xb7\xb7" } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 1,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 1 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);

    xlog "Search for U+5DA3 which doesn't seem to have a name";
    $res = $self->{store}->xconvmultisort(search => [ 'body', { Literal => "\xe5\xb6\xa3" } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 1,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 2 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);

    xlog "Search for U+C0AC HANGUL SYLLABLE SA";
    $res = $self->{store}->xconvmultisort(search => [ 'body', { Literal => "\xec\x82\xac" } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 1,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 3 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);

    xlog "Search for U+8BF6 which doesn't seem to have a name";
    $res = $self->{store}->xconvmultisort(search => [ 'subject', { Literal => "\xe8\xaf\xb6" } ])
	or die "XCONVMULTISORT failed: $@";
    delete $res->{highestmodseq} if defined $res;
    $self->assert_deep_equals({
	total => 1,
	position => 1,
	xconvmulti => [ [ $mboxname_ext, 4 ] ],
	uidvalidity => { $mboxname_ext => $uidvalidity }
    }, $res);
}

sub test_sphinx_xconvmultisort_russian
{
    my ($self) = @_;

    xlog "Test the XCONVMULTISORT command with Russian";
    xlog "characters [IRIS-2048]";
    $self->xconvmultisort_russian_test_common();
}

sub test_xapian_xconvmultisort_russian
{
    my ($self) = @_;

    xlog "Test the XCONVMULTISORT command with Russian";
    xlog "characters [IRIS-2048]";
    $self->xconvmultisort_russian_test_common();
}

sub xconvmultisort_russian_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname_ext, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "Append messages";
    my %exp;
    # The subject translates as "In Soviet Russia, snippets highlight you"
    $exp{A} = $self->make_message('=?utf-8?q?' .
	'=D0=92 ' .
	'=D0=A1=D0=BE=D0=B2=D0=B5=D1=82=D1=81=D0=BA=D0=BE=D0=B9 ' .
	'=D0=A0=D0=BE=D1=81=D1=81=D0=B8=D0=B8 ' .
	'=D1=84=D1=80=D0=B0=D0=B3=D0=BC=D0=B5=D0=BD=D1=82=D0=B5 ' .
	'=D0=BF=D0=BE=D0=B4=D1=87=D0=B5=D1=80=D0=BA=D0=B8=D0=B2=D0=B0=D0=B5=D1=82=D1=81=D1=8F ' .
	'=D0=B2=D1=8B' .
	'?=');

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivvvv', $mboxname_int);

    xlog "Check the messages got there";
    $self->check_messages(\%exp);

    my @queries = (
	[ "\xd0\x92",
	  "<b>\xd0\x92</b> " .
	  "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9 " .
	  "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8 " .
	  "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5 " .
	  "\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f " .
	  "\xd0\xb2\xd1\x8b" ],
	[ "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9",
	  "\xd0\x92 " .
	  "<b>\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9</b> " .
	  "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8 " .
	  "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5 " .
	  "\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f " .
	  "\xd0\xb2\xd1\x8b" ],
	# same word but with U+438 instead of U+439 as the last letter
	# to test Sphinx's normalisation tables with XSNIPPETS
	[ "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb8",
	  "\xd0\x92 " .
	  "<b>\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9</b> " .
	  "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8 " .
	  "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5 " .
	  "\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f " .
	  "\xd0\xb2\xd1\x8b" ],
	[ "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8",
	  "\xd0\x92 " .
	  "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9 " .
	  "<b>\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8</b> " .
	  "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5 " .
	  "\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f " .
	  "\xd0\xb2\xd1\x8b" ],
	[ "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5",
	  "\xd0\x92 " .
	  "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9 " .
	  "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8 " .
	  "<b>\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5</b> " .
	  "\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f " .
	  "\xd0\xb2\xd1\x8b" ],
	[
	"\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f",
	  "\xd0\x92 " .
	  "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9 " .
	  "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8 " .
	  "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5 " .
	  "<b>\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f</b> " .
	  "\xd0\xb2\xd1\x8b" ],
	[ "\xd0\xb2\xd1\x8b",
	  "\xd0\x92 " .
	  "\xd0\xa1\xd0\xbe\xd0\xb2\xd0\xb5\xd1\x82\xd1\x81\xd0\xba\xd0\xbe\xd0\xb9 " .
	  "\xd0\xa0\xd0\xbe\xd1\x81\xd1\x81\xd0\xb8\xd0\xb8 " .
	  "\xd1\x84\xd1\x80\xd0\xb0\xd0\xb3\xd0\xbc\xd0\xb5\xd0\xbd\xd1\x82\xd0\xb5 " .
	  "\xd0\xbf\xd0\xbe\xd0\xb4\xd1\x87\xd0\xb5\xd1\x80\xd0\xba\xd0\xb8\xd0\xb2\xd0\xb0\xd0\xb5\xd1\x82\xd1\x81\xd1\x8f " .
	  "<b>\xd0\xb2\xd1\x8b</b>" ]
    );

    foreach my $q (@queries)
    {
	xlog "Search for word \"" . $q->[0] . "\"";
	$res = $self->{store}->xconvmultisort(search => [ 'subject', { Literal => $q->[0] } ])
	    or die "XCONVMULTISORT failed: $@";
	delete $res->{highestmodseq} if defined $res;
	$self->assert_deep_equals({
	    total => 1,
	    position => 1,
	    xconvmulti => [ [ $mboxname_ext, 1 ] ],
	    uidvalidity => { $mboxname_ext => $uidvalidity }
	}, $res);
    }

    foreach my $q (@queries)
    {
	xlog "Run XSNIPPETS, word \"" . $q->[0] . "\"";
	$res = $self->{store}->xsnippets(
	    "(($mboxname_ext $uidvalidity (1)))",
	    'utf-8',
	    'subject', { Literal => $q->[0] })
	    or die "XSNIPPETS failed: $@";
	delete $res->{highestmodseq} if defined $res;
	$self->assert_deep_equals({
	    snippet => [
		{
		    mailbox => $mboxname_ext,
		    uidvalidity => $uidvalidity,
		    uid => 1,
		    part => 'SUBJECT',
		    snippet => $q->[1],
		}
	    ]
	}, $res);
    }
}

# Note, Squat does not support XSNIPPETS

sub test_sphinx_xsnippets
    :XConvMultiSort
{
    my ($self) = @_;

    xlog "Test the XSNIPPETS command";
    $self->xsnippets_test_common();
}

sub test_xapian_xsnippets
    :XConvMultiSort
{
    my ($self) = @_;

    xlog "Test the XSNIPPETS command";
    $self->xsnippets_test_common();
}

sub xsnippets_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname_int = 'user.cassandane';
    my $mboxname_ext = 'INBOX';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname_ext, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "Append messages";
    my %exp;
    $exp{A} = $self->make_message('synth cred',
				  from => Cassandane::Address->new(
					    name => "Denim",
					    localpart => 'scenester',
					    domain => 'banksy.com'
				  ),
				  body => "occupy ethical\r\n");

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname_int);

    xlog "Check the messages got there";
    $self->check_messages(\%exp);

    my @cases = (
	{
	    query => [ 'from', 'banksy' ],
	    expected => [
		[ 'FROM', 'Denim &lt;scenester@<b>banksy</b>.com&gt;' ]
	    ]
	},
	{
	    query => [ 'subject', 'cred' ],
	    expected => [
		[ 'SUBJECT', 'synth <b>cred</b>' ]
	    ]
	},
	{
	    query => [ 'body', 'ethical' ],
	    expected => [
		[ 'BODY', 'occupy <b>ethical</b> ' ]
	    ]
	},
	# TEXT matches any field
	{
	    query => [ 'text', 'ethical' ],
	    expected => [
		[ 'BODY', 'occupy <b>ethical</b> ' ]
	    ]
	},
	{
	    query => [ 'text', 'cred' ],
	    expected => [
		[ 'SUBJECT', 'synth <b>cred</b>' ],
		[ 'HEADERS', "Transfer-Encoding: 7bit " .
			     "Subject: synth <b>cred</b> " .
			     "From: Denim &lt;scenester\@banksy." ]
	    ]
	},
	{
	    query => [ 'text', 'denim' ],
	    expected => [
		[ 'FROM', '<b>Denim</b> &lt;scenester@banksy.com&gt;' ],
		[ 'HEADERS', "7bit " .
			     "Subject: synth cred " .
			     "From: <b>Denim</b> &lt;scenester\@banksy.com&gt; " .
			     "Message-" ]
	    ]
	},
	{
	    query => [ 'from', 'banksy', 'subject', 'cred' ],
	    expected => [
		[ 'FROM', 'Denim &lt;scenester@<b>banksy</b>.com&gt;' ],
		[ 'SUBJECT', 'synth <b>cred</b>' ]
	    ]
	},
	{
	    query => [ 'or', 'from', 'banksy', 'subject', 'cred' ],
	    expected => [
		[ 'FROM', 'Denim &lt;scenester@<b>banksy</b>.com&gt;' ],
		[ 'SUBJECT', 'synth <b>cred</b>' ]
	    ]
	},
    );

    foreach my $c (@cases)
    {
	xlog "Run XSNIPPETS, query " . join(' ', @{$c->{query}});
	$res = $self->{store}->xsnippets(
	    "(($mboxname_ext $uidvalidity (1)))",
	    'utf-8',
	    @{$c->{query}})
	    or die "XSNIPPETS failed: $@";
	delete $res->{highestmodseq} if defined $res;
	$self->assert_deep_equals({
	    snippet => [
		map {
		    {
			mailbox => $mboxname_ext,
			uidvalidity => $uidvalidity,
			uid => 1,
			part => $_->[0],
			snippet => $_->[1],
		    };
		} @{$c->{expected}}
	    ]
	}, $res);
    }
}

sub write_synclog_file
{
    my ($synclogfile, @mboxes) = @_;

    open SYNCLOG, '>', $synclogfile
	or die "Cannot open $synclogfile for writing: $!";
    foreach my $mbox (@mboxes)
    {
	printf SYNCLOG "APPEND %s\n", $mbox;
    }
    close SYNCLOG;
}

sub expected_dump
{
    my ($base, $uidv, %counts) = @_;

    my $exp = {};
    foreach my $f (keys %counts)
    {
	my $count = $counts{$f};
	$exp->{"$base.$f"} = { $uidv->{$f} => { map { $_ => 1 } (1..$count) } };
    }
    return $exp;
}

sub test_squatter_synclog_mode_sphinx
    :RollingSquatter
{
    my ($self) = @_;
    xlog "test squatter synclog mode with Sphinx";
    $self->squatter_synclog_mode_test_common();
}

sub test_squatter_synclog_mode_xapian
    :RollingSquatter
{
    my ($self) = @_;
    xlog "test squatter synclog mode with Sphinx";
    $self->squatter_synclog_mode_test_common();
}

sub squatter_synclog_mode_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $base_ext = 'INBOX';
    my $base_int = 'user.cassandane';
    my @folders = ( 'etsy', 'quinoa', 'dreamcatcher',
		    'brooklyn', 'shoreditch', 'williamsburg' );

    xlog "Creating folders";
    my %uidv;
    foreach my $f (@folders)
    {
	my $folder = "$base_ext.$f";
	$talk->create($folder)
	    or die "Cannot create $folder: $@";
	my $res = $talk->status($folder, ['uidvalidity']);
	$uidv{$f} = $res->{uidvalidity};
    }

    my %exp = ( map { $_ => {} } @folders );

    xlog "$folders[0] will remain empty";

    my $f = $folders[1];
    xlog "$f will have one indexed message";
    $self->{store}->set_folder("$base_ext.$f");
    $self->{gen}->set_next_uid(1);
    $exp{$f}->{A} = $self->make_message("Message A");
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$base_int.$f");

    $f = $folders[2];
    xlog "$f will have two indexed messages";
    $self->{store}->set_folder("$base_ext.$f");
    $self->{gen}->set_next_uid(1);
    $exp{$f}->{B} = $self->make_message("Message B");
    $exp{$f}->{C} = $self->make_message("Message C");
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$base_int.$f");

    $f = $folders[3];
    xlog "$f will have one indexed message and one unindexed";
    $self->{store}->set_folder("$base_ext.$f");
    $self->{gen}->set_next_uid(1);
    $exp{$f}->{D} = $self->make_message("Message D");
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', "$base_int.$f");
    $exp{$f}->{E} = $self->make_message("Message E");

    $f = $folders[4];
    xlog "$f will have two unindexed messages";
    $self->{store}->set_folder("$base_ext.$f");
    $self->{gen}->set_next_uid(1);
    $exp{$f}->{F} = $self->make_message("Message F");
    $exp{$f}->{G} = $self->make_message("Message G");

    $f = $folders[5];
    xlog "$f will have two unindexed messages";
    $self->{store}->set_folder("$base_ext.$f");
    $self->{gen}->set_next_uid(1);
    $exp{$f}->{H} = $self->make_message("Message H");
    $exp{$f}->{I} = $self->make_message("Message I");

    xlog "Check the messages got there";
    foreach my $folder (@folders)
    {
	$self->{store}->set_folder("$base_ext.$folder");
	$self->check_messages($exp{$folder});
    }

    xlog "Check that the messages we expect were indexed";
    my $res = index_dump($self->{instance});
    my $expdump = expected_dump($base_int, \%uidv,
	# no messages in $folders[0]
	$folders[1] => 1,
	$folders[2] => 2,
	$folders[3] => 1,
	# no indexed messages in $folders[4]
	# no indexed messages in $folders[5]
    );
    $self->assert_deep_equals($expdump, $res);

    xlog "Build an empty synclog file";
    my $synclogfile = $self->{instance}->{basedir} . "/test.synclog";
    write_synclog_file($synclogfile);

    xlog "Run squatter in synclog mode";
    $self->{instance}->run_command({ cyrus => 1 },
		    'squatter', '-v', '-f', $synclogfile);

    xlog "Check that synclogfile still exists";
    $self->assert( -f $synclogfile );

    xlog "Check that no more messages were indexed";
    $res = index_dump($self->{instance});
    $self->assert_deep_equals(expected_dump($base_int, \%uidv,
	# no messages in $folders[0]
	$folders[1] => 1,
	$folders[2] => 2,
	$folders[3] => 1,
	# no indexed messages in $folders[4]
	# no indexed messages in $folders[5]
    ), $res);

    xlog "Build a synclog file mentioning $folders[5] only";
    write_synclog_file($synclogfile, "$base_int.$folders[5]");

    xlog "Run squatter in synclog mode";
    $self->{instance}->run_command({ cyrus => 1 },
		    'squatter', '-v', '-f', $synclogfile);

    xlog "Check that synclogfile still exists";
    $self->assert( -f $synclogfile );

    xlog "Check that $folders[5] was indexed and no others";
    $res = index_dump($self->{instance});
    $self->assert_deep_equals(expected_dump($base_int, \%uidv,
	# no messages in $folders[0]
	$folders[1] => 1,
	$folders[2] => 2,
	$folders[3] => 1,
	# no indexed messages in $folders[4]
	$folders[5] => 2
    ), $res);

    xlog "Build a synclog file mentioning all the folders";
    write_synclog_file($synclogfile, map { "$base_int.$_" } @folders);

    xlog "Run squatter in synclog mode";
    $self->{instance}->run_command({ cyrus => 1 },
		    'squatter', '-v', '-f', $synclogfile);

    xlog "Check that synclogfile still exists";
    $self->assert( -f $synclogfile );

    xlog "Check that all folders were indexed";
    $res = index_dump($self->{instance});
    $self->assert_deep_equals(expected_dump($base_int, \%uidv,
	# no messages in $folders[0]
	$folders[1] => 1,
	$folders[2] => 2,
	$folders[3] => 2,
	$folders[4] => 2,
	$folders[5] => 2,
    ), $res);
}

sub test_index_headers_sphinx
    :NoIndexHeaders
{
    my ($self) = @_;
    xlog "test the search_index_headers config option with Sphinx";
    $self->index_headers_test_common();
}

sub test_index_headers_xapian
    :NoIndexHeaders
{
    my ($self) = @_;
    xlog "test the search_index_headers config option with Xapian";
    $self->index_headers_test_common();
}

sub index_headers_test_common
{
    my ($self) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    my $uid = 1;
    $exp{$uid} = $self->make_filter_message($filter_data[0], $uid);
    $uid++;

    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    my @tests = (
	# body is indexed
	{ query => 'body:pickled', expected => [ 1 ] },
	{ query => 'body:sartorial', expected => [ 1 ] },
	{ query => 'body:beer', expected => [ 1 ] },
	# Subject header is indexed
	{ query => 'subject:umami', expected => [ 1 ] },
	# To header is indexed
	{ query => 'to:viral', expected => [ 1 ] },
	{ query => 'to:mixtape', expected => [ 1 ] },
	# From header is indexed
	{ query => 'from:etsy', expected => [ 1 ] },
	# Cc header is indexed
	{ query => 'cc:cred', expected => [ 1 ] },
	# Bcc header is indexed
	{ query => 'bcc:streetart', expected => [ 1 ] },
	# other headers are *not* indexed if search_index_headers=no
	{ query => 'header:narwhal', expected => [ ] },
	{ query => 'header:butcher', expected => [ ] },
    );
    foreach my $t (@tests)
    {
	xlog "Testing query \"$t->{query}\"";
	$res = run_squatter($self->{instance}, '-vv', '-e', $t->{query}, $mboxname);
	$self->assert_deep_equals({
	    $mboxname => {
		map { $_ => 1 } @{$t->{expected}}
	    }
	}, $res);
    }

}

sub test_xapian_squatter_contenttype
{
    my ($self) = @_;

    xlog "Test the indexing of MIME content types";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  mime_type => 'multipart/mixed',
				  mime_boundary => 'COSBY-SWEATER',
				  body =>
"--COSBY-SWEATER\r\n" .
"\r\n" .
"Quinoa etsy\r\n" .
"--COSBY-SWEATER\r\n" .
"Content-Type: image/jpeg\r\n" .
"\r\n" .
"/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAA0JCgwKCA0MCwwPDg0QFCIWFBISFCkdHxgiMSszMjAr\r\n" .
"Ly42PE1CNjlJOi4vQ1xESVBSV1dXNEFfZl5UZU1VV1P/2wBDAQ4PDxQSFCcWFidTNy83U1NTU1NT\r\n" .
"U1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1P/wAARCABVAEcDASIA\r\n" .
"AhEBAxEB/8QAHAAAAgIDAQEAAAAAAAAAAAAAAAQFBgIDBwEI/8QAPBAAAgEDAgMFBQUECwAAAAAA\r\n" .
"AQIDAAQREiEFBjEHE0FRYSJxkaHBFCOBsdEVMkNyF0JTYnOCksLS4fH/xAAXAQEBAQEAAAAAAAAA\r\n" .
"AAAAAAACAAED/8QAIBEAAgICAgMBAQAAAAAAAAAAAAECERIhAzEiQXETYf/aAAwDAQACEQMRAD8A\r\n" .
"6dXhIoPSqLzNzU0ty9hw2UoiZ7+dOp8MKff41Gxi5OkWjivHeHcKQm7uVVx/DX2nP4D61Uz2koLl\r\n" .
"h+zmMB/cPegMfeMY+dUy4gBQsmrvMZfX1b1FIMQBkjO/QeFJJMUoOLo6b/SBAy6hw26I/mWtMPaJ\r\n" .
"E92Em4dJHbnqwfLD8MfWubBk1YwM9PWs9Wk4G4O21bSB/Du9jfW3ELdZ7SVZY28VPQ+RHgaZBzXH\r\n" .
"OF3N5wmbvLacpOf4Y3XHkw8fpXSeWuYLfjtoJI8RzoMSw53U+Y9KHfQ58codk1RRRUAgec7+XhvL\r\n" .
"dzLA2mV8RqfLJ3x+Ga5PFPptSyKDqOT7sHb55q+9p9wVsLO2BwJJS5/yjH1rmiydzMjEBlB2U7il\r\n" .
"ViUsdofubx5EjJc5AABxuB1rSGU+3o1KpOxHX09KYW4uyVCyae8GQdIAAHlSq6hK/eliCzKxPnnr\r\n" .
"npTSV2cpuSWLVDixAksTmEfea9t18vfnalHZVcyBQmDkADIX0z50x3DiQxZGgN+9nb/3NLSOGZY0\r\n" .
"J0qcA4zv51tJov0k5LyvQ1ZX5jRjqBLDBJ6+/wB9N8r8Qbh/M8EinETS6GC+Ibb4bj4VHRzzs57w\r\n" .
"LLoGkKV2NK28hUq6jDDDD08qFI65Nx2u9n0GKKW4fcre2Fvcp+7NGr/EZooBOd9qEuvilnGr7pES\r\n" .
"QPDLf9VXODWwm4fxcyxK4itQVkPVDrXAHv3+FN9oNy8/NdxHKCBCqoiluoxnPxNMcmxpLwHmIO0m\r\n" .
"BAuV/wBRB9+1P0VWV+UqAuhskAePT3fKs7N5CZI3YoD7TlWxg+BrQ0bW9zpeNwVxkOMb1n37GWUI\r\n" .
"iqjODoK536eNJUc5pkkIY+67wzfeZ3i1DI2zr1dKSuXdH3y4jAIy2c5/rUyYU0HDeZ1dRjyx5UhF\r\n" .
"Kr+1KAAIyAF2B3z0+NZFUtux8rtrxod4FbyXNzDGpJ7x8svrgn6VGL7Gz7EbEVYuz5O85otzklED\r\n" .
"sAT/AHTiobjStFxy8Vk9oSt02x41mjU5Ps6v2f3X2nlaBf7F2j+eR8iKKieyyV24ZexsAFSUEbeY\r\n" .
"33/AUUWRVu0W0EHMszHJ79Fkz5HGNvhW7kaRI7PjisZDi115ONIC5+e4+dTXana5SxuAOjMmfeMj\r\n" .
"8jVHteIyWVlfQRgFbuIRsfIBgfyzW9ovhtN215xMSzsJHbxA2U0sWLXMhYEapNx4gZ6VkwWAFkOW\r\n" .
"I2JAIoik151hGkzq1Edd6WsaQbk5Z+2OGFRLLOZPu2TCqD19PwpGy0yyKrqrDSQcj0pySDRCuGLA\r\n" .
"47sFhg+BpSB0QyRYxnxU/rQgo+jpzynJU0Wbs+lt4uNpCyMZHLqknnsTj4D5VWuNu8nHb5pg+szv\r\n" .
"kdMb7fKneXC9pfwXRYhYZ0yQei6hn5VnzkwPNnESABlxjHuG9JJJhUsl8Lj2UBf2bf6ST98vX+Wi\r\n" .
"tnZYh/ZN7JjAafHwUfrRRZEpz7Zm75ZuCF1NDiRceh/TNcewMHI9k/pX0DdQJcW7xSDKOCCPQ1yH\r\n" .
"nPlx+F8TP2K0m+w92uHUMyg75ya1MitxuXhAY7+JrO3meIMEdlJ9klTjI8RS6h1AGravdCnOehpE\r\n" .
"NxyvGwdCFYHY56VqLAXCs2+diD41oWFAMYzXunT0GCPA1hEhY3GmaWF9IEgwuehPhSzyyzSO8hLS\r\n" .
"D2Sx3Jxt9K0ltY3GG86n+UOXX5gmuRJMY1hAOcZJLE/oa2wpNM6B2boF5VRh1eVy3vzj6UVNcC4X\r\n" .
"HwbhcVlE5dUydbDBJJz9aK5sRI1iN96KKiIjiHK/BuIszXFhFrPV0GhviKh5OzjgznKy3iegkB/M\r\n" .
"UUVtka27NOEkbXV6D56l/wCNYJ2acMXZry8b8UH+2iiohy37P+AwqNcM1x/izH8hgVOcK4Lw7hCy\r\n" .
"Lw+1SAOQW0knPxoorDCQoooqNP/Z\r\n" .
"--COSBY-SWEATER--\r\n");

    xlog "check the message got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => { $uidvalidity => { 1 => 1 } } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:text', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:plain', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:text_plain', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:image', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:jpeg', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:image_jpeg', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:image_gif', $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'contenttype:application', $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);
}

sub test_xapian_search_contenttype
{
    my ($self) = @_;

    xlog "Test the searching of MIME content types";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append a message";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  mime_type => 'multipart/mixed',
				  mime_boundary => 'COSBY-SWEATER',
				  body =>
"--COSBY-SWEATER\r\n" .
"\r\n" .
"Quinoa etsy\r\n" .
"--COSBY-SWEATER\r\n" .
"Content-Type: image/jpeg\r\n" .
"\r\n" .
"/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAA0JCgwKCA0MCwwPDg0QFCIWFBISFCkdHxgiMSszMjAr\r\n" .
"Ly42PE1CNjlJOi4vQ1xESVBSV1dXNEFfZl5UZU1VV1P/2wBDAQ4PDxQSFCcWFidTNy83U1NTU1NT\r\n" .
"U1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1NTU1P/wAARCABVAEcDASIA\r\n" .
"AhEBAxEB/8QAHAAAAgIDAQEAAAAAAAAAAAAAAAQFBgIDBwEI/8QAPBAAAgEDAgMFBQUECwAAAAAA\r\n" .
"AQIDAAQREiEFBjEHE0FRYSJxkaHBFCOBsdEVMkNyF0JTYnOCksLS4fH/xAAXAQEBAQEAAAAAAAAA\r\n" .
"AAAAAAACAAED/8QAIBEAAgICAgMBAQAAAAAAAAAAAAECERIhAzEiQXETYf/aAAwDAQACEQMRAD8A\r\n" .
"6dXhIoPSqLzNzU0ty9hw2UoiZ7+dOp8MKff41Gxi5OkWjivHeHcKQm7uVVx/DX2nP4D61Uz2koLl\r\n" .
"h+zmMB/cPegMfeMY+dUy4gBQsmrvMZfX1b1FIMQBkjO/QeFJJMUoOLo6b/SBAy6hw26I/mWtMPaJ\r\n" .
"E92Em4dJHbnqwfLD8MfWubBk1YwM9PWs9Wk4G4O21bSB/Du9jfW3ELdZ7SVZY28VPQ+RHgaZBzXH\r\n" .
"OF3N5wmbvLacpOf4Y3XHkw8fpXSeWuYLfjtoJI8RzoMSw53U+Y9KHfQ58codk1RRRUAgec7+XhvL\r\n" .
"dzLA2mV8RqfLJ3x+Ga5PFPptSyKDqOT7sHb55q+9p9wVsLO2BwJJS5/yjH1rmiydzMjEBlB2U7il\r\n" .
"ViUsdofubx5EjJc5AABxuB1rSGU+3o1KpOxHX09KYW4uyVCyae8GQdIAAHlSq6hK/eliCzKxPnnr\r\n" .
"npTSV2cpuSWLVDixAksTmEfea9t18vfnalHZVcyBQmDkADIX0z50x3DiQxZGgN+9nb/3NLSOGZY0\r\n" .
"J0qcA4zv51tJov0k5LyvQ1ZX5jRjqBLDBJ6+/wB9N8r8Qbh/M8EinETS6GC+Ibb4bj4VHRzzs57w\r\n" .
"LLoGkKV2NK28hUq6jDDDD08qFI65Nx2u9n0GKKW4fcre2Fvcp+7NGr/EZooBOd9qEuvilnGr7pES\r\n" .
"QPDLf9VXODWwm4fxcyxK4itQVkPVDrXAHv3+FN9oNy8/NdxHKCBCqoiluoxnPxNMcmxpLwHmIO0m\r\n" .
"BAuV/wBRB9+1P0VWV+UqAuhskAePT3fKs7N5CZI3YoD7TlWxg+BrQ0bW9zpeNwVxkOMb1n37GWUI\r\n" .
"iqjODoK536eNJUc5pkkIY+67wzfeZ3i1DI2zr1dKSuXdH3y4jAIy2c5/rUyYU0HDeZ1dRjyx5UhF\r\n" .
"Kr+1KAAIyAF2B3z0+NZFUtux8rtrxod4FbyXNzDGpJ7x8svrgn6VGL7Gz7EbEVYuz5O85otzklED\r\n" .
"sAT/AHTiobjStFxy8Vk9oSt02x41mjU5Ps6v2f3X2nlaBf7F2j+eR8iKKieyyV24ZexsAFSUEbeY\r\n" .
"33/AUUWRVu0W0EHMszHJ79Fkz5HGNvhW7kaRI7PjisZDi115ONIC5+e4+dTXana5SxuAOjMmfeMj\r\n" .
"8jVHteIyWVlfQRgFbuIRsfIBgfyzW9ovhtN215xMSzsJHbxA2U0sWLXMhYEapNx4gZ6VkwWAFkOW\r\n" .
"I2JAIoik151hGkzq1Edd6WsaQbk5Z+2OGFRLLOZPu2TCqD19PwpGy0yyKrqrDSQcj0pySDRCuGLA\r\n" .
"47sFhg+BpSB0QyRYxnxU/rQgo+jpzynJU0Wbs+lt4uNpCyMZHLqknnsTj4D5VWuNu8nHb5pg+szv\r\n" .
"kdMb7fKneXC9pfwXRYhYZ0yQei6hn5VnzkwPNnESABlxjHuG9JJJhUsl8Lj2UBf2bf6ST98vX+Wi\r\n" .
"tnZYh/ZN7JjAafHwUfrRRZEpz7Zm75ZuCF1NDiRceh/TNcewMHI9k/pX0DdQJcW7xSDKOCCPQ1yH\r\n" .
"nPlx+F8TP2K0m+w92uHUMyg75ya1MitxuXhAY7+JrO3meIMEdlJ9klTjI8RS6h1AGravdCnOehpE\r\n" .
"NxyvGwdCFYHY56VqLAXCs2+diD41oWFAMYzXunT0GCPA1hEhY3GmaWF9IEgwuehPhSzyyzSO8hLS\r\n" .
"D2Sx3Jxt9K0ltY3GG86n+UOXX5gmuRJMY1hAOcZJLE/oa2wpNM6B2boF5VRh1eVy3vzj6UVNcC4X\r\n" .
"HwbhcVlE5dUydbDBJJz9aK5sRI1iN96KKiIjiHK/BuIszXFhFrPV0GhviKh5OzjgznKy3iegkB/M\r\n" .
"UUVtka27NOEkbXV6D56l/wCNYJ2acMXZry8b8UH+2iiohy37P+AwqNcM1x/izH8hgVOcK4Lw7hCy\r\n" .
"Lw+1SAOQW0knPxoorDCQoooqNP/Z\r\n" .
"--COSBY-SWEATER--\r\n");

    xlog "check the message got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => { $uidvalidity => { 1 => 1 } } }, $res);

    xlog "Search with an index";

    $res = $talk->search('xcontenttype', { Quote => 'text' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'plain' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'text_plain' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'image' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'jpeg' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'image_jpeg' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'image_gif' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ ], $res);

    $res = $talk->search('xcontenttype', { Quote => 'application' })
	or die "Cannot search: $@";
    $self->assert_deep_equals([ ], $res);
}

sub test_xapian_squatter_listid
{
    my ($self) = @_;

    xlog "Test the indexing of List-Id and Mailing-List headers";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  extra_headers => [
					[ 'List-Id' => 'sustainable quinoa' ]
				  ]);
    $exp{2} = $self->make_message("Message 2",
				  extra_headers => [
					[ 'Mailing-List' => 'mustache dreamcatcher' ]
				  ]);
    $exp{3} = $self->make_message("Message 3",
				  extra_headers => [
					[ 'List-Id' => 'pickled cardigan' ],
					[ 'Mailing-List' => 'narwhal chillwave' ]
				  ]);
    $exp{4} = $self->make_message("Message 4");

    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => {
				$uidvalidity => { map { $_ => 1 } (1..4) }
			    } }, $res);

    xlog "Check the index can be searched";

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:sustainable', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:quinoa', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:mustache', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 2 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:dreamcatcher', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 2 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:pickled', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 3 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:cardigan', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 3 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:narwhal', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 3 => 1 } }, $res);

    $res = run_squatter($self->{instance}, '-vv', '-e', 'listid:chillwave', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 3 => 1 } }, $res);
}

sub test_xapian_search_listid
{
    my ($self) = @_;

    xlog "Test the searching of List-Id and Mailing-List headers";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  extra_headers => [
					[ 'List-Id' => 'sustainable quinoa' ]
				  ]);
    $exp{2} = $self->make_message("Message 2",
				  extra_headers => [
					[ 'Mailing-List' => 'mustache dreamcatcher' ]
				  ]);
    $exp{3} = $self->make_message("Message 3",
				  extra_headers => [
					[ 'List-Id' => 'pickled cardigan' ],
					[ 'Mailing-List' => 'narwhal chillwave' ]
				  ]);
    $exp{4} = $self->make_message("Message 4");

    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => {
				$uidvalidity => { map { $_ => 1 } (1..4) }
			    } }, $res);

    xlog "Check the index can be searched";

    $res = $talk->search('xlistid', 'sustainable')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xlistid', 'quinoa')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('xlistid', 'mustache')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 2 ], $res);

    $res = $talk->search('xlistid', 'dreamcatcher')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 2 ], $res);

    $res = $talk->search('xlistid', 'pickled')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 3 ], $res);

    $res = $talk->search('xlistid', 'cardigan')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 3 ], $res);

    $res = $talk->search('xlistid', 'narwhal')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 3 ], $res);

    $res = $talk->search('xlistid', 'chillwave')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 3 ], $res);
}

sub test_xapian_search_headers
    :NoIndexHeaders
{
    my ($self) = @_;

    xlog "Test the HEADER search criterion";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    xlog "append some messages";
    my %exp;
    $exp{1} = $self->make_message("Message 1",
				  extra_headers => [
					[ 'Gastropub' => 'mumblecore selvage' ]
				  ]);
    $exp{2} = $self->make_message("Message 2",
				  extra_headers => [
					[ 'Wayfarers' => 'helvetica mustache' ]
				  ]);
    $exp{3} = $self->make_message("Message 3",
				  extra_headers => [
					[ 'Wayfarers' => 'squid mustache' ],
				  ]);
    $exp{4} = $self->make_message("Message 4");

    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    $res = index_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({ $mboxname => {
				$uidvalidity => { map { $_ => 1 } (1..4) }
			    } }, $res);

    xlog "Check the index can be searched";

    $res = $talk->search('header', 'gastropub', 'mumblecore')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('header', 'gastropub', 'selvage')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 1 ], $res);

    $res = $talk->search('header', 'wayfarers', 'helvetica')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 2 ], $res);

    $res = $talk->search('header', 'wayfarers', 'mustache')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 2, 3 ], $res);

    $res = $talk->search('header', 'wayfarers', 'squid')
	or die "Cannot search: $@";
    $self->assert_deep_equals([ 3 ], $res);
}


1;

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
use Data::Dumper;

use lib '.';
use base qw(Cassandane::Cyrus::TestCase);
use Cassandane::Util::Log;

sub new
{
    my $class = shift;
    return $class->SUPER::new({}, @_);
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

sub config_squatter_squat
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=squat";
    $conf->set(search_engine => 'squat');
}

sub test_squatter_squat
{
    my ($self) = @_;

    xlog "test squatter with SQUAT";
    $self->squatter_test_common(\&squat_dump);
}

sub sphinx_dump
{
    my ($instance, $mbox) = @_;

    my $filename = $instance->{basedir} . "/sphinx_dump.out";
    my $sock = $instance->{basedir} . '/conf/socket/sphinx.cassandane';

    $instance->run_command(
	    { redirects => { stdout => $filename } },
	    'mysql',
	    '--socket', $sock,
	    '--batch',
	    '--raw',
	    '-e', 'SELECT cyrusid FROM rt LIMIT 1000'
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
	next if (defined $mbox && $mboxname ne $mbox);
	$res->{$mboxname} ||= {};
	$res->{$mboxname}->{$uidvalidity} ||= {};
	$res->{$mboxname}->{$uidvalidity}->{$uid} = 1;
    }
    close RESULTS;

    return $res;
}

sub config_squatter_sphinx
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');

    $conf->set(search_batchsize => '3');
}

sub test_squatter_sphinx
{
    my ($self) = @_;

    xlog "test squatter with Sphinx";
    $self->squatter_test_common(\&sphinx_dump);
}

sub squatter_test_common
{
    my ($self, $dumper) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);
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
    $res = $dumper->($self->{instance}, $mboxname);
    $self->assert_deep_equals({}, $res);

    xlog "First index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the first index run";
    $res = $dumper->($self->{instance}, $mboxname);
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
    my $res2 = $dumper->($self->{instance}, $mboxname);
    $self->assert_deep_equals($res, $res2);

    xlog "Add another message";
    my $uid = $N1+1;
    $exp{$uid} = $self->make_message("Message $uid");

    xlog "Third index run";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "The third run should have indexed the new message";
    $res = $dumper->($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1+1)
		}
	    }
	}, $res);

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_prefilter_squat
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=squat";
    $conf->set(search_engine => 'squat');
}

sub test_prefilter_squat
{
    my ($self) = @_;

    xlog "test squatter with Squat";
    $self->prefilter_test_common();
}

sub config_prefilter_sphinx
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
}

sub test_prefilter_sphinx
{
    my ($self) = @_;

    xlog "test squatter with Sphinx";
    $self->prefilter_test_common();
}

sub index_dump
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
);

sub prefilter_test_common
{
    my ($self, $dumper) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

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
	$res = index_dump($self->{instance}, '-vv', '-e', $t->{query}, $mboxname);
	$self->assert_deep_equals({
	    $mboxname => {
		map { $_ => 1 } @{$t->{expected}}
	    }
	}, $res);
    }

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub rolling_test_common
{
    my ($self, $dumper) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);
    $self->{sync_client_pid} = $self->{instance}->run_command(
		    { cyrus => 1, background => 1},
		    'squatter', '-v', '-R', '-f');

    xlog "appending a message";
    my %exp;
    $exp{1} = $self->make_message("Message A");

    xlog "check the messages got there";
    $self->check_messages(\%exp);

    $self->replication_wait('squatter');

    xlog "Indexer should have indexed the message";
    $res = $dumper->($self->{instance}, $mboxname);
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
    $res = $dumper->($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    map { $_ => 1 } (1..$N1)
		}
	    }
	}, $res);

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_rolling_squat
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=squat";
    $conf->set(search_engine => 'squat');
    xlog "Setting sync_log = yes";
    $conf->set(sync_log => 'yes');
    xlog "Setting sync_log_channels = squatter";
    $conf->set(sync_log_channels => 'squatter');
}

sub test_rolling_squat
{
    my ($self) = @_;

    xlog "test squatter rolling mode with Squat";
    $self->rolling_test_common(\&squat_dump);
}

sub config_rolling_sphinx
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
    xlog "Setting sync_log = yes";
    $conf->set(sync_log => 'yes');
    xlog "Setting sync_log_channels = squatter";
    $conf->set(sync_log_channels => 'squatter');
}

sub test_rolling_sphinx
{
    my ($self) = @_;

    xlog "test squatter rolling mode with Sphinx";
    $self->rolling_test_common(\&sphinx_dump);
}

sub config_8bit_sphinx
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
}

sub test_8bit_sphinx
{
    my ($self) = @_;

    xlog "test indexing 8bit characters with Sphinx";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);
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
    $res = index_dump($self->{instance}, '-vv', '-e', 'body:hello', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
    $res = index_dump($self->{instance}, '-vv', '-e', 'body:world', $mboxname);
    $self->assert_deep_equals({ $mboxname => { 1 => 1 } }, $res);
    $res = index_dump($self->{instance}, '-vv', '-e', 'body:quinoa', $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_sphinx_query_limit
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
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

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

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

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_sphinx_large_query
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
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

    xlog "test truncation of large messages";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

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
    $res = sphinx_dump($self->{instance}, $mboxname);
    $self->assert_deep_equals({
	    $mboxname => {
		$uidvalidity => {
		    1 => 1,
		    2 => 1,
		}
	    }
	}, $res);

    xlog "Check that the untruncated text is found for both messages";
    $res = index_dump($self->{instance}, '-vv', '-e',
		      'body:' .  encode_number($untruncated_n), $mboxname);
    $self->assert_deep_equals({
	$mboxname => {
	    1 => 1,
	    2 => 1
	}
    }, $res);

    xlog "Check that the truncated text is not found";
    $res = index_dump($self->{instance}, '-vv', '-e',
		      'body:' .  encode_number($truncated_n), $mboxname);
    $self->assert_deep_equals({ $mboxname => { } }, $res);

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_prefilter_multi
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
}

sub test_prefilter_multi
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

    xlog "start the sphinx daemon";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

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
	$res = index_dump($self->{instance}, '-vvm', '-e', $t->{query}, $mboxname);

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

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_xconvmultisort
{
    my ($self, $conf) = @_;
    #
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');
    xlog "Setting conversations=on";
    $conf->set(conversations => 'on',
	       conversations_db => 'twoskip');
    # XCONVMULTISORT only works on Sphinx anyway
}

sub test_xconvmultisort
{
    my ($self) = @_;

    xlog "test the XCONVMULTISORT command";
    # Note, Squat does not support multiple folder searching

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    my @folders = ( 'kale', 'tofu', 'smallbatch' );

    xlog "create folders";
    foreach my $folder (@folders)
    {
	$talk->create("$mboxname.$folder")
	    or die "Cannot create folder $mboxname.$folder: $@";
    }

    xlog "start the sphinx daemon";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

    xlog "append some messages";
    my $exp = {};
    map { $exp->{$_} = {}; } @folders;
    my $uid = 1;
    my $hms = 6;
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
	$hms++;
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

	my @search;
	if ($t->{query} =~ m/header:/)
	{
	    # no direct equivalent
	    next;
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

	$res = $self->{store}->xconvmultisort(search => [ @search ]);
	xlog "res = " . Dumper($res);

	my $exp = {
	    highestmodseq => $hms,
	    total => scalar(@{$t->{expected}})
	};
	foreach my $i (@{$t->{expected}})
	{
	    my $folder = "INBOX." . $folders[($i-1) % scalar(@folders)];
	    my $uid = int(($i-1) / scalar(@folders)) + 1;
	    $exp->{xconvmulti} ||= {};
	    $exp->{xconvmulti}->{$folder} ||= [];
	    push(@{$exp->{xconvmulti}->{$folder}}, $uid);
	}
	xlog "expecting " . Data::Dumper::Dumper($exp);

	$self->assert_deep_equals($exp, $res);
    }

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

sub config_iris1936
{
    my ($self, $conf) = @_;
    xlog "Setting search_engine=sphinx";
    $conf->set(search_engine => 'sphinx');

    $conf->set(search_batchsize => '3');
}

sub test_iris1936
{
    my ($self) = @_;

    xlog "Regression test for IRIS-1936, where squatter with Sphinx";
    xlog "would loop forever multiply indexing the first 20 messages";
    xlog "in a mailbox which was the 21st mailbox with the same";
    xlog "uidvalidity";

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

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
	$self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
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
    $self->assert_deep_equals($iexp, sphinx_dump($self->{instance}));

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'stop', $mboxname);
}

1;

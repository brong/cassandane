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
	    '-e', 'SELECT cyrusid FROM rt'
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

sub prefilter_test_common
{
    my ($self, $dumper) = @_;

    my $talk = $self->{store}->get_client();
    my $mboxname = 'user.cassandane';

    my $res = $talk->status($mboxname, ['uidvalidity']);
    my $uidvalidity = $res->{uidvalidity};

    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-v', '-c', 'start', $mboxname);

    xlog "append some messages";
    # data thanks to hipsteripsum.me
    my @data = (
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
    my %exp;
    my $uid = 1;
    foreach my $d (@data)
    {
	my $to = Cassandane::Address->new(
		    name => "Test User $d->{to}",
		    localpart => 'test',
		    domain => 'vmtom.com'
		);
	my $from = Cassandane::Generator::make_random_address(extra => " $d->{from}");
	my $cc = Cassandane::Generator::make_random_address(extra => " $d->{cc}");
	my $bcc = Cassandane::Generator::make_random_address(extra => " $d->{bcc}");

	$exp{$uid} = $self->make_message($d->{subject} . " [$uid]",
					 body => $d->{body} . "\r\n",
					 to => $to,
					 from => $from,
					 cc => $cc,
					 bcc => $bcc,
					 extra_headers => [ [ 'Narwhal', $d->{narwhal} ] ]
					 );
	$uid++;
    }
    xlog "check the messages got there";
    $self->check_messages(\%exp);

    xlog "Index the messages";
    $self->{instance}->run_command({ cyrus => 1 }, 'squatter', '-ivv', $mboxname);

    xlog "Check the results of the index run";
    my @tests = (
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
    foreach my $t (@tests)
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

1;

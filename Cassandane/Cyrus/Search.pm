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
package Cassandane::Cyrus::Search;
use base qw(Cassandane::Cyrus::TestCase);
use Cwd qw(abs_path);
use DateTime;
use Cassandane::Util::Log;
use Data::Dumper;

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

1;

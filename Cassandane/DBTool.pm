#!/usr/bin/perl
#
#  Copyright (c) 2011-2012 Opera Software Australia Pty. Ltd.  All rights
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

package Cassandane::DBTool;
use strict;
use warnings;
use Cassandane::Util::Log;
use Cassandane::Instance;

#
# Return a new dbtool object which can be used to get/set/delete values
# from a Cyrus database using the cyr_dbtool utility.  Useful for
# fiddling databases behind Cyrus' back.
#
sub new
{
    my ($class, $instance, $dbname, $dbformat) = @_;

    $dbname = $instance->{basedir} . "/" . $dbname
	unless $dbname =~ m/^\//;
    $dbformat ||= 'skiplist';

    return bless {
	instance => $instance,
	dbname => $dbname,
	dbformat => $dbformat,
    }, $class;
}

#
# Run dbtool, optionally capturing the output
#
sub _run_dbtool
{
    my ($self, $want_output, @args) = @_;

    my $run_params = { cyrus => 1 };
    my $outfile;
    my $output;

    if ($want_output)
    {
	$outfile = $self->{instance}->{basedir} . "/dbtool.out";
	$run_params->{redirects} = { stdout => $outfile };
    }

    $self->{instance}->run_command($run_params,
		       'cyr_dbtool', $self->{dbname}, $self->{dbformat}, @args);

    if ($want_output)
    {
	open OUTPUT, '<', $outfile
	    or die "Cannot open $outfile for reading: $!";
	$output = join('', readline(OUTPUT));
	close OUTPUT;
    }

    return $output;
}

#
# Get a value from the database
#
sub get
{
    my ($self, $key) = @_;
    return $self->_run_dbtool(1, 'get', $key);
}

#
# Set a value to the database.
#
sub set
{
    my ($self, $key, $data) = @_;
    $self->_run_dbtool(0, 'set', $key, $data);
}

#
# Delete a value from the given database.
#
sub delete
{
    my ($self, $key) = @_;
    $self->_run_dbtool(0, 'delete', $key);
}



1;

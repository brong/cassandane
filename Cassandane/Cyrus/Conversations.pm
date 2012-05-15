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
package Cassandane::Cyrus::Conversations;
use base qw(Cassandane::Cyrus::TestCase);
use DateTime;
use URI::Escape;
use File::stat;
use Cassandane::ThreadedGenerator;
use Cassandane::Util::Log;
use Cassandane::DBTool;
use Cassandane::Util::DateTime qw(to_iso8601 from_iso8601
				  from_rfc822
				  to_rfc3501 from_rfc3501);

sub new
{
    my ($class, @args) = @_;
    my $config = Cassandane::Config->default()->clone();
    $config->set(conversations => 'on',
		 conversations_db => 'twoskip');
    return $class->SUPER::new({ config => $config }, @args);
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
    $self->{store}->set_fetch_attributes('uid', 'cid');
}

sub tear_down
{
    my ($self) = @_;
    $self->SUPER::tear_down();
}

# The resulting CID when a clash happens is supposed to be
# the MAXIMUM of all the CIDs.  Here we use the fact that
# CIDs are expressed in a form where lexical order is the
# same as numeric order.
sub choose_cid
{
    my (@cids) = @_;
    @cids = sort { $b cmp $a } @cids;
    return $cids[0];
}

sub test_append_unrelated
{
    my ($self) = @_;
    my %exp;

    xlog "Test APPEND of unrelated messages to IMAP, which form";
    xlog "singleton conversations.";

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Message B");
    $exp{B}->set_attributes(cid => $exp{B}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message C";
    $exp{C} = $self->make_message("Message C");
    $exp{C}->set_attributes(cid => $exp{C}->make_cid());
    my $actual = $self->check_messages(\%exp);

    xlog "generating message D";
    $exp{D} = $self->make_message("Message D");
    $exp{D}->set_attributes(cid => $exp{D}->make_cid());
    $self->check_messages(\%exp);

    $self->assert_str_not_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{D}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{D}->cid());
    $self->assert_str_not_equals($exp{C}->cid(), $exp{D}->cid());
}

sub test_append_related
{
    my ($self) = @_;
    my %exp;

    xlog "Test APPEND of messages related by both message-id and";
    xlog "subject, which form one large conversation.";

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Re: Message A",
				  references => [ $exp{A} ]);
    $exp{B}->set_attributes(cid => $exp{A}->cid());
    $self->check_messages(\%exp);

    xlog "generating message C";
    $exp{C} = $self->make_message("RE: Message A",
				  references => [ $exp{A} ]);
    $exp{C}->set_attributes(cid => $exp{A}->cid());
    my $actual = $self->check_messages(\%exp);

    xlog "generating message D";
    $exp{D} = $self->make_message("Re: Re: Message A",
				  references => [ $exp{C} ]);
    $exp{D}->set_attributes(cid => $exp{C}->cid());
    $self->check_messages(\%exp);

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{C}->cid(), $exp{D}->cid());
}


sub test_append_subject
{
    my ($self) = @_;
    my %exp;

    xlog "Test APPEND of messages related by message-id but";
    xlog "with different subject, which form two conversations.";

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Re: Message A",
				  references => [ $exp{A} ]);
    $exp{B}->set_attributes(cid => $exp{A}->cid());
    $self->check_messages(\%exp);

    xlog "generating message C";
    $exp{C} = $self->make_message("I like to change subjects",
				  references => [ $exp{A} ]);
    $exp{C}->set_attributes(cid => $exp{C}->make_cid());
    my $actual = $self->check_messages(\%exp);

    xlog "generating message D";
    $exp{D} = $self->make_message("Re: I like to change subjects",
				  references => [ $exp{C} ]);
    $exp{D}->set_attributes(cid => $exp{C}->cid());
    $self->check_messages(\%exp);

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{D}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{C}->cid(), $exp{D}->cid());
}

sub test_subject_variability
{
    my ($self) = @_;
    my %exp;

    xlog "Test APPEND of messages related by message-id but";
    xlog "with subjects differing in various ways, to see which";
    xlog "of those differences forms a new conversation.";

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    my $id = 1;
    $exp{$id} = $self->make_message("Message A");
    $exp{$id}->set_attributes(cid => $exp{1}->make_cid());
    $self->check_messages(\%exp);

    my @cases = (
	{ subject => "Message A", same_cid => 1 },
	{ subject => "Re: Message A", same_cid => 1 },
	{ subject => "Re: Re: Message A", same_cid => 1 },
	{ subject => "RE: SV: Re: Sv: Message A", same_cid => 1 },
	{ subject => "Fwd: RE: Re: Sv: Message A", same_cid => 1 },
	{ subject => "Message   A", same_cid => 1 },
	{ subject => " Message   A", same_cid => 1 },
	{ subject => " Message \t A", same_cid => 1 },
	{ subject => "MessageA", same_cid => 1 },
	{ subject => "Message a", same_cid => 0 },
	{ subject => "message A", same_cid => 0 },
	{ subject => "[BLOB] Message A", same_cid => 1 },
	{ subject => "[blobby Blob blob] Message A", same_cid => 1 },
	{ subject => "  [blobby Blob blob] Message A", same_cid => 1 },
	{ subject => "  [blobby Blob blob]\tMessage A", same_cid => 1 },
	{ subject => "  [blobby Blob blob]Message A", same_cid => 1 },
	{ subject => "Message A [trailing blob]", same_cid => 0 },
	{ subject => "Message [interpolated blob] A", same_cid => 0 },
	{ subject => "Message A (fwd)", same_cid => 0 },    # sigh
	{ subject => "=?utf-8?Q?Message_A?=", same_cid => 1 },
	{ subject => "=?utf-8?Q?Message=20A?=", same_cid => 1 },
	{ subject => "=?utf-8?Q?Mess?= =?utf-8?Q?age=20A?=", same_cid => 1 },
    );

    foreach my $case (@cases)
    {
	xlog "generating message for subject \"" . $case->{subject} . "\"";
	$id++;
	$exp{$id} = $self->make_message($case->{subject}, references => [ $exp{1} ]);
	if ($case->{same_cid})
	{
	    $exp{$id}->set_attribute(cid => $exp{1}->cid());
	}
	else
	{
	    $exp{$id}->set_attribute(cid => $exp{$id}->make_cid());
	    $self->assert_str_not_equals($exp{1}->cid(), $exp{$id}->cid());
	}
	$self->check_messages(\%exp, keyed_on => 'uid');
    }
}

sub test_append_subject_rfc2047
{
    my ($self) = @_;
    my %exp;

    xlog "Test APPEND of messages related by message-id but";
    xlog "with different subject, which form two conversations,";
    xlog "where the subjects are identical after RFC2047 decoding";
    xlog "but differ before it, due to vagaries in different encoders.";

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    my $subject1 = "=?utf-8?Q?Dropbox_funksjonalitet_p=C3=A5_M?=\r\n=?utf-8?Q?y_Opera/Browse_Me?=";
    my $subject2 = "=?UTF-8?Q?Re:=20Dropbox=20funksj?=\r\n=?UTF-8?Q?onalitet=20p=C3=A5=20My=20Ope?=\r\n=?UTF-8?Q?ra/Browse=20Me?=";

    xlog "generating message A";
    $exp{A} = $self->make_message($subject1);
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp, keyed_on => 'uid');

    xlog "generating message B";
    $exp{B} = $self->make_message($subject2,
				  references => [ $exp{A} ]);
    $exp{B}->set_attributes(cid => $exp{A}->cid());
    $self->check_messages(\%exp, keyed_on => 'uid');

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
}


#
# Test APPEND of messages to IMAP which results in a CID clash.
#
sub test_append_clash
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Re: Message A");
    $exp{B}->set_attributes(cid => $exp{B}->make_cid());
    my $actual = $self->check_messages(\%exp);

    $self->assert_str_not_equals($exp{A}->cid(), $exp{B}->cid());

    xlog "generating message C";
    $exp{C} = $self->make_message("Re: Re: Message A",
				  references => [ $exp{A}, $exp{B} ],
				 );
    my $ElCid = choose_cid($exp{A}->cid(), $exp{B}->cid());
    $exp{A}->set_attributes(cid => $ElCid);
    $exp{B}->set_attributes(cid => $ElCid);
    $exp{C}->set_attributes(cid => $ElCid);

    $self->check_messages(\%exp);

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{C}->cid());
}

#
# Test APPEND of messages to IMAP which results in multiple CID clashes.
#
sub test_double_clash
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Re: Message A");
    $exp{B}->set_attributes(cid => $exp{B}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message C";
    $exp{C} = $self->make_message("Re: Re: Message A");
    $exp{C}->set_attributes(cid => $exp{C}->make_cid());
    my $actual = $self->check_messages(\%exp);

    $self->assert_str_not_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{C}->cid());

    xlog "generating message D";
    $exp{D} = $self->make_message("Re: Re: Re: Message A",
				  references => [ $exp{A}, $exp{B}, $exp{C} ],
				 );
    my $ElCid = choose_cid($exp{A}->cid(), $exp{B}->cid(), $exp{C}->cid());
    $exp{A}->set_attributes(cid => $ElCid);
    $exp{B}->set_attributes(cid => $ElCid);
    $exp{C}->set_attributes(cid => $ElCid);
    $exp{D}->set_attributes(cid => $ElCid);

    $self->check_messages(\%exp);

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{C}->cid(), $exp{D}->cid());
}

#
# Test that a CID clash resolved on the master is replicated
#
sub test_replication_clash
{
    my ($self) = @_;
    my %exp;

    xlog "need a master and replica pair";
    $self->assert_not_null($self->{replica});
    my $master_store = $self->{master_store};
    my $replica_store = $self->{replica_store};

    $master_store->set_fetch_attributes('uid', 'cid');
    $replica_store->set_fetch_attributes('uid', 'cid');

    # Double check that we're connected to the servers
    # we wanted to be connected to.
    $self->assert($master_store->{host} eq $replica_store->{host});
    $self->assert($master_store->{port} != $replica_store->{port});

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($master_store->get_client()->capability()->{xconversations});
    $self->assert($replica_store->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A", store => $master_store);
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->run_replication();
    $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    xlog "generating message B";
    $exp{B} = $self->make_message("Re: Message A", store => $master_store);
    $exp{B}->set_attributes(cid => $exp{B}->make_cid());
    $self->run_replication();
    $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    xlog "generating message C";
    $exp{C} = $self->make_message("Re: Re: Message A", store => $master_store);
    $exp{C}->set_attributes(cid => $exp{C}->make_cid());
    $self->run_replication();
    my $actual = $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    $self->assert_str_not_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{C}->cid());

    xlog "generating message D";
    $exp{D} = $self->make_message("Re: Re: Re: Message A",
				  store => $master_store,
				  references => [ $exp{A}, $exp{B}, $exp{C} ],
				 );
    my $ElCid = choose_cid($exp{A}->cid(), $exp{B}->cid(), $exp{C}->cid());
    $exp{A}->set_attributes(cid => $ElCid);
    $exp{B}->set_attributes(cid => $ElCid);
    $exp{C}->set_attributes(cid => $ElCid);
    $exp{D}->set_attributes(cid => $ElCid);

    $self->run_replication();
    # Since IRIS-293 was reverted, it now takes *two*
    # replication runs to propagate the CID renames.
    $self->run_replication();

    $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{A}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{C}->cid());
    $self->assert_str_equals($exp{B}->cid(), $exp{D}->cid());
    $self->assert_str_equals($exp{C}->cid(), $exp{D}->cid());
}

sub test_xconvfetch
{
    my ($self) = @_;
    my $store = $self->{store};

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($store->get_client()->capability()->{xconversations});

    xlog "generating messages";
    my $generator = Cassandane::ThreadedGenerator->new();
    $store->write_begin();
    while (my $msg = $generator->generate())
    {
	$store->write_message($msg);
    }
    $store->write_end();

    xlog "reading the whole folder again to discover CIDs etc";
    my %cids;
    my %uids;
    $store->read_begin();
    while (my $msg = $store->read_message())
    {
	my $uid = $msg->get_attribute('uid');
	my $cid = $msg->get_attribute('cid');
	my $threadid = $msg->get_header('X-Cassandane-Thread');
	if (defined $cids{$cid})
	{
	    $self->assert_num_equals($threadid, $cids{$cid});
	}
	else
	{
	    $cids{$cid} = $threadid;
	    xlog "Found CID $cid";
	}
	$self->assert_null($uids{$uid});
	$uids{$uid} = 1;
    }
    $store->read_end();

    xlog "Using XCONVFETCH on each conversation";
    foreach my $cid (keys %cids)
    {
	xlog "XCONVFETCHing CID $cid";

	my $result = $store->xconvfetch_begin($cid);
	$self->assert_not_null($result->{xconvmeta});
	$self->assert_num_equals(1, scalar keys %{$result->{xconvmeta}});
	$self->assert_not_null($result->{xconvmeta}->{$cid});
	$self->assert_not_null($result->{xconvmeta}->{$cid}->{modseq});
	while (my $msg = $store->xconvfetch_message())
	{
	    my $muid = $msg->get_attribute('uid');
	    my $mcid = $msg->get_attribute('cid');
	    my $threadid = $msg->get_header('X-Cassandane-Thread');
	    $self->assert_str_equals($cid, $mcid);
	    $self->assert_num_equals($cids{$cid}, $threadid);
	    $self->assert_num_equals(1, $uids{$muid});
	    $uids{$muid} |= 2;
	}
	$store->xconvfetch_end();
    }

    xlog "checking that all the UIDs in the folder were XCONVFETCHed";
    foreach my $uid (keys %uids)
    {
	$self->assert_num_equals(3, $uids{$uid});
    }
}

#
# Test APPEND of a new composed draft message to the Drafts folder by
# the Fastmail webui, which sets the X-ME-Message-ID header to thread
# conversations but not any of Message-ID, References, or In-Reply-To.
#
sub test_fm_webui_draft
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->{gen}->generate(subject => 'Draft message A');
    $exp{A}->remove_headers('Message-ID');
    $exp{A}->add_header('X-ME-Message-ID', '<fake1700@fastmail.fm>');
    $exp{A}->set_attribute(cid => $exp{A}->make_cid());

    $self->{store}->write_begin();
    $self->{store}->write_message($exp{A});
    $self->{store}->write_end();
    $self->check_messages(\%exp, keyed_on => 'uid');

    xlog "generating message B";
    $exp{B} = $exp{A}->clone();
    # avoid changing the subject
    $exp{B}->set_body("Completely different text here\r\n");
    $exp{B}->set_attribute(uid => 2);
    # clone() gave B the same cid as A

    $self->{store}->write_begin();
    $self->{store}->write_message($exp{B});
    $self->{store}->write_end();
    $self->check_messages(\%exp, keyed_on => 'uid');

    xlog "generating message C";
    $exp{C} = $exp{A}->clone();
    $exp{C}->set_headers('Subject', 'Draft message was once A');
    $exp{C}->set_body("Even more completely different text here\r\n");
    $exp{C}->set_attributes(uid => 3, cid => $exp{C}->make_cid());

    $self->{store}->write_begin();
    $self->{store}->write_message($exp{C});
    $self->{store}->write_end();
    $self->check_messages(\%exp, keyed_on => 'uid');

    $self->assert_str_equals($exp{A}->cid(), $exp{B}->cid());
    $self->assert_str_not_equals($exp{A}->cid(), $exp{C}->cid());
    $self->assert_str_not_equals($exp{B}->cid(), $exp{C}->cid());
}

#
# Test a COPY between folders owned by different users
#
sub test_cross_user_copy
{
    my ($self) = @_;
    my $bobuser = "bob";
    my $bobfolder = "user.$bobuser";

    xlog "Testing COPY between folders owned by different users [IRIS-893]";

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    my $srv = $self->{instance}->get_service('imap');

    $self->{instance}->create_user($bobuser);

    my $adminstore = $srv->create_store(username => 'admin');
    my $adminclient = $adminstore->get_client();
    $adminclient->setacl('user.cassandane', $bobuser => 'lrswipkxtecda')
	or die "Cannot setacl on user.cassandane: $@";

    xlog "generating two messages";
    my %exp;
    $exp{A} = $self->{gen}->generate(subject => 'Message A');
    my $cid = $exp{A}->make_cid();
    $exp{A}->set_attribute(cid => $cid);
    $exp{B} = $self->{gen}->generate(subject => 'Re: Message A',
				     references => [ $exp{A} ]);
    $exp{B}->set_attribute(cid => $cid);

    xlog "Writing messaged to user.cassandane";
    $self->{store}->write_begin();
    $self->{store}->write_message($exp{A});
    $self->{store}->write_message($exp{B});
    $self->{store}->write_end();
    xlog "Check that the messages made it";
    $self->check_messages(\%exp);

    my $bobstore = $srv->create_store(username => $bobuser);
    $bobstore->set_fetch_attributes('uid', 'cid');
    my $bobclient = $bobstore->get_client();
    $bobstore->set_folder('user.cassandane');
    $bobstore->_select();
    $bobclient->copy(2, $bobfolder)
	or die "Cannot COPY message to $bobfolder";

    xlog "Check that the message made it to $bobfolder";
    my %bobexp;
    $bobexp{B} = $exp{B}->clone();
    $bobexp{B}->set_attributes(uid => 1, cid => $exp{B}->make_cid());
    $bobstore->set_folder($bobfolder);
    $self->check_messages(\%bobexp, store => $bobstore);
}

sub max
{
    my $max;
    map { $max = $_ if (!defined $max || $_ > $max); } @_;
    return $max;
}

sub check_status
{
    my ($self, $folder, %expected) = @_;

    my @stores = ('store');
    if (defined $self->{replica_store})
    {
	$self->run_replication();
	push(@stores, 'replica_store');
    }

    foreach my $sname (@stores)
    {
	xlog "checking STATUS, folder:$folder store:$sname";
	my $status = $self->{$sname}->get_client()->status($folder, [keys %expected]);
	$self->assert_deep_equals(\%expected, $status);
    }
}

sub test_status
{
    my ($self) = @_;

    xlog "Testing extended STATUS items";

    my $talk = $self->{store}->get_client();
    my %exp;
    # With conversations, modseqs are per-user
    my %ms = ( user => 4, inbox => 4 );

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($talk->capability()->{xconversations});

    xlog "Check the STATUS response, initially empty inbox";
    $self->check_status('inbox',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Create a 2nd folder";
    $talk->create('inbox.sub') || die "Cannot create inbox.sub: $@";
    $ms{inboxsub} = ++$ms{user};

    xlog "Check the STATUS response, initially empty inbox.sub";
    $self->check_status('inbox.sub',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inboxsub},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Add 1st message";
    $exp{A} = $self->make_message("Message A");
    $ms{conv1} = $ms{inbox} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 1,
		unseen => 1,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Mark the message read";
    $talk->store('1', '+flags', '(\\Seen)');
    $ms{inbox} = $ms{conv1} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 1,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 0,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Add 2nd message";
    $exp{B} = $self->make_message("Message B");
    $self->assert_str_not_equals($exp{A}->make_cid(), $exp{B}->make_cid());
    $ms{inbox} = $ms{conv2} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 2,
		unseen => 1,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 1,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );

    xlog "Add 3rd message, in the 1st conversation";
    $exp{C} = $self->make_message("Re: Message A",
				  references => [ $exp{A} ]);
    $ms{inbox} = $ms{conv1} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 3,
		unseen => 2,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 2,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );

    xlog "Double check the STATUS for inbox.sub";
    $self->check_status('inbox.sub',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inboxsub},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Add a message to inbox.sub, in the 1st conversation";
    $self->{store}->set_folder('inbox.sub');
    $self->{gen}->set_next_uid(1);
    $exp{D} = $self->make_message("Re: [D] Message A",
				  references => [ $exp{A} ]);
    $ms{inboxsub} = $ms{conv1} = ++$ms{user};

    xlog "Check the STATUS response for inbox";
    $self->check_status('inbox',
		messages => 3,
		unseen => 2,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 2,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );
    xlog "Check the STATUS response for inbox.sub";
    $self->check_status('inbox.sub',
		messages => 1,
		unseen => 1,
		highestmodseq => $ms{inboxsub},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Mark all messages in inbox read";
    $self->{store}->set_folder('inbox');
    $self->{store}->_select();
    $talk->store('1:*', '+flags', '(\\Seen)');
    $ms{inbox} = $ms{conv1} = $ms{conv2} = ++$ms{user};

    xlog "Check the STATUS response for inbox";
    $self->check_status('inbox',
		messages => 3,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 1,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );
    xlog "Check the STATUS response for inbox.sub";
    $self->check_status('inbox.sub',
		messages => 1,
		unseen => 1,
		highestmodseq => $ms{inboxsub},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv2},
	    );
}

sub test_status_replication
{
    my ($self) = @_;

    xlog "Testing replication of extended STATUS items";

    my $mstore = $self->{master_store};
    my $rstore = $self->{replica_store};
    my %exp;
    # With conversations, modseqs are per-user
    my %ms = ( user => 4, inbox => 4 );

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($mstore->get_client()->capability()->{xconversations});
    $self->assert($rstore->get_client()->capability()->{xconversations});

    xlog "Check the STATUS response, initially empty inbox";
    $self->check_status('inbox',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Create a 2nd folder";
    $mstore->get_client()->create('inbox.sub') || die "Cannot create inbox.sub: $@";
    $ms{inboxsub} = ++$ms{user};

    xlog "Check the STATUS response, initially empty inbox.sub";
    $self->check_status('inbox.sub',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inboxsub},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Add 1st message";
    $exp{A} = $self->make_message("Message A");
    $ms{conv1} = $ms{inbox} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 1,
		unseen => 1,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Mark the message read";
    $mstore->get_client()->store('1', '+flags', '(\\Seen)');
    $ms{inbox} = $ms{conv1} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 1,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 0,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Add 2nd message";
    $exp{B} = $self->make_message("Message B");
    $self->assert_str_not_equals($exp{A}->make_cid(), $exp{B}->make_cid());
    $ms{inbox} = $ms{conv2} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 2,
		unseen => 1,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 1,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );

    xlog "Add 3rd message, in the 1st conversation";
    $exp{C} = $self->make_message("Re: Message A",
				  references => [ $exp{A} ]);
    $ms{inbox} = $ms{conv1} = ++$ms{user};

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 3,
		unseen => 2,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 2,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );

    xlog "Double check the STATUS for inbox.sub";
    $self->check_status('inbox.sub',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inboxsub},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Add a message to inbox.sub, in the 1st conversation";
    $self->{store}->set_folder('inbox.sub');
    $self->{gen}->set_next_uid(1);
    $exp{D} = $self->make_message("Re: [D] Message A",
				  references => [ $exp{A} ]);
    $ms{inboxsub} = $ms{conv1} = ++$ms{user};

    xlog "Check the STATUS response for inbox";
    $self->check_status('inbox',
		messages => 3,
		unseen => 2,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 2,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );
    xlog "Check the STATUS response for inbox.sub";
    $self->check_status('inbox.sub',
		messages => 1,
		unseen => 1,
		highestmodseq => $ms{inboxsub},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Mark all messages in inbox read";
    $self->{store}->set_folder('inbox');
    $self->{store}->_select();
    $mstore->get_client()->store('1:*', '+flags', '(\\Seen)');
    $ms{inbox} = $ms{conv1} = $ms{conv2} = ++$ms{user};

    xlog "Check the STATUS response for inbox";
    $self->check_status('inbox',
		messages => 3,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 2,
		xconvunseen => 1,
		xconvmodseq => max($ms{conv1}, $ms{conv2}),
	    );
    xlog "Check the STATUS response for inbox.sub";
    $self->check_status('inbox.sub',
		messages => 1,
		unseen => 1,
		highestmodseq => $ms{inboxsub},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv2},
	    );
}

sub test_status_replication_expunged_msg
{
    my ($self) = @_;

    xlog "Test replication of an xconvmodseq STATUS item";
    xlog "when the xconvmodseq is ahead of all current messages";
    xlog "due to the most recent message being deleted and";
    xlog "expunged [IRIS-1182]";

    my $mstore = $self->{master_store};
    my $rstore = $self->{replica_store};
    my %exp;
    # With conversations, modseqs are per-user
    my %ms = ( user => 4, inbox => 4 );

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($mstore->get_client()->capability()->{xconversations});
    $self->assert($rstore->get_client()->capability()->{xconversations});

    xlog "Check the STATUS response, initially empty inbox";
    $self->check_status('inbox',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Add 3 messages in one conversation";
    $exp{A} = $self->make_message("Message A");
    $exp{B} = $self->make_message("Re: Message A", references => [ $exp{A} ]);
    $exp{C} = $self->make_message("Re: Re: Message A", references => [ $exp{B} ]);
    $ms{conv1} = $ms{inbox} = ($ms{user} += 3);

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 3,
		unseen => 3,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Add one more message to the conversation";
    $exp{D} = $self->make_message("Re: Re: Re: Message A", references => [ $exp{C} ]);
    xlog "Delete and expunge the message again";
    $mstore->get_client()->store(4, '+flags', '(\\Deleted)');
    $mstore->get_client()->expunge();
    # Do a delayed expunge run to force the mailbox to repack
    $self->run_delayed_expunge();
    $ms{inbox} = $ms{conv1} = ($ms{user} += 3);

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 3,
		unseen => 3,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );
}

sub test_status_replication_expunged_msg_b
{
    my ($self) = @_;

    xlog "Test replication of an xconvmodseq STATUS item";
    xlog "when the xconvmodseq is ahead on the replica";

    my $mstore = $self->{master_store};
    my $rstore = $self->{replica_store};
    my %exp;
    # With conversations, modseqs are per-user
    my %ms = ( user => 4, inbox => 4 );

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($mstore->get_client()->capability()->{xconversations});
    $self->assert($rstore->get_client()->capability()->{xconversations});

    xlog "Check the STATUS response, initially empty inbox";
    $self->check_status('inbox',
		messages => 0,
		unseen => 0,
		highestmodseq => $ms{inbox},
		xconvexists => 0,
		xconvunseen => 0,
		xconvmodseq => 0,
	    );

    xlog "Add 3 messages in one conversation";
    $exp{A} = $self->make_message("Message A");
    $exp{B} = $self->make_message("Re: Message A", references => [ $exp{A} ]);
    $exp{C} = $self->make_message("Re: Re: Message A", references => [ $exp{B} ]);
    $ms{conv1} = $ms{inbox} = ($ms{user} += 3);

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 3,
		unseen => 3,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );

    xlog "Add one more message to the conversation";
    $exp{D} = $self->make_message("Re: Re: Re: Message A",
				  store => $rstore,
				  references => [ $exp{C} ]);
    xlog "Delete and expunge the message again";
    $rstore->get_client()->store(4, '+flags', '(\\Deleted)');
    $rstore->get_client()->expunge();
    # Do a delayed expunge run to force the mailbox to repack
    $self->run_delayed_expunge(instance => $self->{replica});
    $ms{conv1} = ($ms{user} += 3);
    # highestmodseq gets bumped by one extra in this case
    $ms{inbox} = $ms{user} + 1;

    xlog "Check the STATUS response";
    $self->check_status('inbox',
		messages => 3,
		unseen => 3,
		highestmodseq => $ms{inbox},
		xconvexists => 1,
		xconvunseen => 1,
		xconvmodseq => $ms{conv1},
	    );
}

sub run_conversations_audit
{
    my ($self, $user) = @_;
    $user ||= 'cassandane';

    xlog "Set the conversations db mtime backwards";
    my $conv_db = $self->{instance}->{basedir} . "/conf/user/c/cassandane.conversations";
    my $st = stat($conv_db)
	or die "Cannot stat $conv_db: $!";
    my $mtime = $st->mtime - 10;
    my $size = $st->size;

    utime($mtime, $mtime, $conv_db)
	or die "Cannot set mtime back into the past on $conv_db: $!";
    $st = stat($conv_db)
	or die "Cannot stat $conv_db: $!";
    $self->assert_num_equals($mtime, $st->mtime);
    $self->assert_num_equals($size, $st->size);
    $self->assert_num_equals($mtime, $st->atime);


    xlog "Run the conversations audit command";
    my $outfile = $self->{instance}->{basedir} . "/audit.out";
    unlink($outfile);
    $self->{instance}->run_command({
		cyrus => 1,
		redirects => {
		    stdout => $outfile,
		},
	    }, 'ctl_conversationsdb', '-v', '-A', $user);
    open OUTPUT, '<', $outfile
	or die "Cannot open $outfile for reading: $!";
    my @output = readline(OUTPUT);
    my $output = join('', @output);
    close OUTPUT;

    foreach my $s (@output) { chomp $s; xlog "output: $s"; }

    xlog "Check the conversations db is undamaged by the audit";
    $st = stat($conv_db)
	or die "Cannot stat $conv_db: $!";
    $self->assert_num_equals($mtime, $st->mtime, "Conversations DB has not been written");
    $self->assert_num_equals($size, $st->size, "Conversations DB is the same size");
    $self->assert_num_not_equals($mtime, $st->atime, "Conversations DB has been read");

    xlog "Check that audit did actually check the database";
    $self->assert($output =~ m/^Inbox user.$user$/m);
    $self->assert($output =~ m/^Pass 1:/m);
    $self->assert($output =~ m/^Pass 2:/m);

    return $output;
}

sub test_db_audit
{
    my ($self) = @_;

    xlog "Testing ctl_conversationsdb -A";

    my $store = $self->{store};

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($store->get_client()->capability()->{xconversations});

    xlog "generating messages";
    my $generator = Cassandane::ThreadedGenerator->new(nthreads => 10, nmessages => 35);
    $store->write_begin();
    my %Bkeys;
    while (my $msg = $generator->generate())
    {
	$Bkeys{"B" . $msg->cid()} = 1;
	$store->write_message($msg);
    }
    $store->write_end();
    $self->assert(scalar keys %Bkeys > 5);

    # Don't need an IMAP connection anymore, everything from
    # here on works directly on backend databases.
    $store->disconnect();
    $store = undef;

    my $dbtool = new Cassandane::DBTool($self->{instance},
					'conf/user/c/cassandane.conversations',
					'twoskip');

    my $output = $self->run_conversations_audit();

    xlog "Check that audit did not report any differences";
    $self->assert_does_not_match(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_matches(qr/is OK$/m, $output);
    $self->assert_does_not_match(qr/is BROKEN/m, $output);
    $self->assert_does_not_match(qr/^ADDED /m, $output);
    $self->assert_does_not_match(qr/^MISSING /m, $output);
    $self->assert_does_not_match(qr/^CHANGED /m, $output);

    my ($key_to_change, $key_to_delete, $key_to_add, @junk) = keys %Bkeys;
    # Make $key_to_add a different key from all the others
    my $n = 1;
    while (defined $Bkeys{$key_to_add})
    {
	substr($key_to_add, $n, 1, 'a');
	$n++;
    }

    xlog "Damage the DB by changing a record";
    my $data = $dbtool->get($key_to_change);
    chomp $data;
    $dbtool->set($key_to_change, "XX" . $data . "YY");

    $output = $self->run_conversations_audit();

    xlog "Check that audit reported the correct differences";
    $self->assert_matches(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_does_not_match(qr/is OK$/m, $output);
    $self->assert_matches(qr/is BROKEN \(1 differences\)$/m, $output);
    $self->assert_does_not_match(qr/^ADDED /m, $output);
    $self->assert_does_not_match(qr/^MISSING /m, $output);
    $self->assert_matches(qr/^CHANGED key \"$key_to_change\"/m,
    $output);

    xlog "Damage the DB by deleting a record";
    $dbtool->delete($key_to_delete);

    $output = $self->run_conversations_audit();

    xlog "Check that audit reported the correct differences";
    $self->assert_matches(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_does_not_match(qr/is OK$/m, $output);
    $self->assert_matches(qr/is BROKEN \(2 differences\)$/m, $output);
    $self->assert_does_not_match(qr/^ADDED /m, $output);
    $self->assert_matches(qr/^MISSING key \"$key_to_delete\"/m, $output);
    $self->assert_matches(qr/^CHANGED key \"$key_to_change\"/m, $output);

    xlog "Damage the DB by adding a record";
    $dbtool->set($key_to_add, 'This data is utterly bogus');

    $output = $self->run_conversations_audit();

    xlog "Check that audit reported the correct differences";
    $self->assert_matches(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_does_not_match(qr/is OK$/m, $output);
    $self->assert_matches(qr/is BROKEN \(3 differences\)$/m, $output);
    $self->assert_matches(qr/^ADDED key \"$key_to_add\"/m, $output);
    $self->assert_matches(qr/^MISSING key \"$key_to_delete\"/m, $output);
    $self->assert_matches(qr/^CHANGED key \"$key_to_change\"/m, $output);

    xlog "Damage the DB by trashing \$FOLDER_NAMES";
    $dbtool->set('$FOLDER_NAMES', '()');

    $output = $self->run_conversations_audit();

    xlog "Check that audit reported the correct differences";
    $self->assert_matches(qr/^RECORDS differ/m, $output);
    $self->assert_matches(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_matches(qr/^MISSING \"user.cassandane\" at 0/m, $output);
    $self->assert_does_not_match(qr/is OK$/m, $output);
    $self->assert_matches(qr/is BROKEN \(4 differences\)$/m, $output);
    $self->assert_matches(qr/^ADDED key \"$key_to_add\"/m, $output);
    $self->assert_matches(qr/^MISSING key \"$key_to_delete\"/m, $output);
    $self->assert_matches(qr/^CHANGED key \"$key_to_change\"/m, $output);
}

sub test_db_audit_rename
{
    my ($self) = @_;

    xlog "Testing ctl_conversationsdb -A vs folder rename";

    my $store = $self->{store};
    # test data from hipsteripsum.me
    my $folderA = 'INBOX.hella';
    my $folderB = 'INBOX.flexitarian';

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($store->get_client()->capability()->{xconversations});

    xlog "creating subfolder";
    $store->get_client()->create($folderA)
	or die "Cannot create $folderA: $@";
    $store->set_folder($folderA);

    xlog "generating messages";
    my $generator = Cassandane::ThreadedGenerator->new(nthreads => 10, nmessages => 35);
    $store->write_begin();
    while (my $msg = $generator->generate())
    {
	$store->write_message($msg);
    }
    $store->write_end();

    # Disconnect while auditing the database, just in case
    $store->disconnect();

    my $output = $self->run_conversations_audit();

    xlog "Check that audit did not report any differences";
    $self->assert_does_not_match(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_matches(qr/is OK$/m, $output);
    $self->assert_does_not_match(qr/is BROKEN/m, $output);
    $self->assert_does_not_match(qr/^ADDED /m, $output);
    $self->assert_does_not_match(qr/^MISSING /m, $output);
    $self->assert_does_not_match(qr/^CHANGED /m, $output);

    xlog "rename the folder from IMAP";
    $store->connect();
    $store->get_client()->rename($folderA, $folderB);
    $store->disconnect();

    $output = $self->run_conversations_audit();

    xlog "Check that audit did not report any differences";
    $self->assert_does_not_match(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_matches(qr/is OK$/m, $output);
    $self->assert_does_not_match(qr/is BROKEN/m, $output);
    $self->assert_does_not_match(qr/^ADDED /m, $output);
    $self->assert_does_not_match(qr/^MISSING /m, $output);
    $self->assert_does_not_match(qr/^CHANGED /m, $output);
}

sub test_db_audit_cross
{
    my ($self) = @_;

    xlog "Testing ctl_conversationsdb -A vs cross-folder conversations";

    my $store = $self->{store};
    # test data from hipsteripsum.me
    my @folders = ( qw|INBOX.beard INBOX.occupy INBOX.pinterest INBOX.letterhead| );

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($store->get_client()->capability()->{xconversations});

    xlog "creating subfolders";
    foreach my $f (@folders)
    {
	$store->get_client()->create($f)
	    or die "Cannot create $f: $@";
    }
    my %by_thread;

    xlog "generating messages";
    my $generator = Cassandane::ThreadedGenerator->new(nthreads => 10, nmessages => 35);
    $store->write_begin();
    while (my $msg = $generator->generate())
    {
	my $n = 0 + $by_thread{$msg->cid()}++;
	$store->set_folder($folders[$n % scalar @folders]);
	$store->_select();
	$store->write_message($msg);
    }
    $store->write_end();

    # Disconnect while auditing the database, just in case
    $store->disconnect();

    my $output = $self->run_conversations_audit();

    xlog "Check that audit did not report any differences";
    $self->assert_does_not_match(qr/^RECORDS differ/m, $output);
    $self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
    $self->assert_matches(qr/is OK$/m, $output);
    $self->assert_does_not_match(qr/is BROKEN/m, $output);
    $self->assert_does_not_match(qr/^ADDED /m, $output);
    $self->assert_does_not_match(qr/^MISSING /m, $output);
    $self->assert_does_not_match(qr/^CHANGED /m, $output);

    foreach my $f (@folders)
    {
	xlog "rename folder $f from IMAP";
	$store->connect();
	$store->get_client()->rename($f, $f . "_art");
	$store->disconnect();

	$output = $self->run_conversations_audit();

	xlog "Check that audit did not report any differences";
	$self->assert_does_not_match(qr/^RECORDS differ/m, $output);
	$self->assert_does_not_match(qr/^FOLDER_NAMES differ/m, $output);
	$self->assert_matches(qr/is OK$/m, $output);
	$self->assert_does_not_match(qr/is BROKEN/m, $output);
	$self->assert_does_not_match(qr/^ADDED /m, $output);
	$self->assert_does_not_match(qr/^MISSING /m, $output);
	$self->assert_does_not_match(qr/^CHANGED /m, $output);
    }
}

sub _bogus_nmh_inreplyto
{
    my ($msg) = @_;

    return "Message from " .
	   $msg->from() .
	   " of \"" .
	   $msg->date() .
	   ".\"\n\t" .
	   $msg->messageid();
}

sub test_bogus_in_reply_to
{
    my ($self) = @_;
    my %exp;

    xlog "Test APPEND of messages with broken In-Reply-To as";
    xlog "generated by some versions of the NMH mailer [IRIS-975].";

    xlog "check IMAP server has the XCONVERSATIONS capability";
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    $self->{store}->set_fetch_attributes('uid', 'cid');

    # Real world example of the bogus In-Reply-To header
    # In-Reply-To: Message from Aristotle Pagaltzis <pagaltzis@gmx.de> of "Sun, 01 Nov 2009 04:11:30 BST."
    #    <20091101031130.GA5708@klangraum.plasmasturm.org>

    my $from = Cassandane::Address->new(
		name => 'Aristotle Pagaltzis',
		localpart => 'pagaltzis',
		domain => 'gmx.de');

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A",
	from => $from,
    );
    $exp{A}->set_attributes(cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B, bogus reply to A";
    $exp{B} = $self->make_message("Message B",
	references => $exp{A}->messageid,
	extra_headers => [
	    [ "In-Reply-To" => _bogus_nmh_inreplyto($exp{A}) ],
	]
    );
    $exp{B}->set_attributes(cid => $exp{A}->cid);
    $self->check_messages(\%exp);

    xlog "pre-generating message C, should be unrelated to A or B";
    $self->{gen}->set_next_uid(4);
    $exp{C} = $self->{gen}->generate(
	subject => "Message C",
	from => $from,
    );

    xlog "generating message D, bogus reply to C, unrelated to A or B";
    $self->{gen}->set_next_uid(3);
    $exp{D} = $self->make_message("Message D",
	references => $exp{C}->messageid,
	extra_headers => [
	    [ "In-Reply-To" => _bogus_nmh_inreplyto($exp{C}) ],
	]
    );
    $exp{D}->set_attributes(cid => $exp{D}->make_cid());
    $exp{C}->set_attributes(cid => $exp{D}->cid);

    xlog "APPENDing message C, out of thread order";
    $self->_save_message($exp{C});

    xlog "This will fail with a CID mismatch for D if the server does not ignore the bogus In-Reply-To";
    $self->check_messages(\%exp);
}

1;

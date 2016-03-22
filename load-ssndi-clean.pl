#!/usr/bin/perl -w
# $Id$ - for use with svn version control#
=head1 NAME

load-ssndi - load ssn death index records into a local database

=head1 SYNOPSIS

A script to automatically download and load death index updates into a local database. Uses wget for downloads.

=head1 DESCRIPTION

Operates in two modes:
1) Read a local file (full or update) and load
2) Download a monthly update from the web and load

For mode 1, md5 checking is not done for full files (not provided), optional but recommended for updates
For mode 2, md5 checking is done automatically

The two modes are mutually exclusive

=head1 RECORD FORMAT Details

The record format for the Death Master File is described here:
https://dmf.ntis.gov/recordlayout.pdf

To summarize, each record is 100 char long:
Record Location     Size        Desc
01                  1           (A)dd, (C)hange, (D)elete flag.
                                Blank (' ') for full file.
02-10               9           Social Security Number (SSN)
11-30               20          Last Name
31-34               4           Name Suffix
35-49               15          First Name
50-64               15          Middle Name
65                  1           (V)erified or (P)roof of death obtained
                                May also be (N) or blank for older records
                                or "fairly reliable source"
66-73               8           Date of Death (MM,DD,CC,YY)
74-81               8           Date of Birth (MM,DD,CC,YY)

The following fields (82-93) have been removed as of 11/1/11 because of privacy law revisions. They were:
82-83               2           State/Country Code
84-88               5           Zip - last known residence
89-93               5           Zip - lump sum payment sent to

The remaining fields (94-100) are blank

=head1 AUTHOR

Matthew A. Levin MD, <mlevin@mlevin.net>

=head1 LICENSE

License: GPL-3
http://www.gnu.org/licenses/gpl-3.0.en.html

=head1 DATE
Created 2011-11-22

=cut

my $VERSION = '$Id: load-ssndi.pl 3017 2014-08-18 20:00:32Z zolo $';

use strict;
use subs;

# NOTE: Date::Calc needs to be force installed because of a bad dependency 
#       on Bit::Vector which does not compile and isn't needed for our purposes
#       See http://www.engelschall.com/u/sb/download/
use Date::Calc qw/check_date/;

use DBI;
use DBIx::Log4perl;
use Digest::MD5 qw(md5_hex);
use Getopt::Long;
use HTML::TableExtract;
use Log::Log4perl;
use Log::Log4perl::Level;

my $OPT_verbose = 0;
my $OPT_update;
my $OPT_md5;
my $OPT_commit = 100;           # commit interval
my $OPT_database = 'ssn';
my $OPT_dbhost = 'localhost';
# TODO: fill in your mysql login/pass below
my $OPT_dbuser = '';
my $OPT_dbpass = '';
my $OPT_help;

my $BASE_URL = 'https://dmf.ntis.gov'; # location of monthly update files
my $MONTHLY_URL = $BASE_URL.'/monthly/';
my $DOWNLOAD_URL = $BASE_URL.'/dmldata/monthly';

# directory in which to store downloaded updates
my $DATA_DIR = '/data/death-index';
my $LOG_DIR = "$DATA_DIR/log";

# set up logging
my $LOGDATE = `date +%Y-%m-%d`;
chomp($LOGDATE);
my $LOGFILE = "$LOG_DIR/update-$LOGDATE.log";

my $logconfig = qq/
log4perl.logger.load-ssndi	     = INFO, Screen, File
log4perl.logger.DBIx.Log4perl    = WARN, Screen, File
log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr  = 1
log4perl.appender.Screen.layout  = Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d %p %m%n

log4perl.appender.File         = Log::Log4perl::Appender::File
log4perl.appender.File.filename = $LOGFILE
log4perl.appender.File.stderr  = 1
log4perl.appender.File.layout  = Log::Log4perl::Layout::PatternLayout
log4perl.appender.File.layout.ConversionPattern = %d %p %m%n
/;

my ($dbh, $insert_sth, $delete_sth);
my $CONNECT_STRING = "dbi:mysql:database=$OPT_database;host=$OPT_dbhost;
                         port=3306;mysql_server_prepare=1";

# name of file to load
my $infile;

################################# MAIN ##################################

Log::Log4perl->init(\$logconfig);
my $L = Log::Log4perl->get_logger('load-ssndi');

# clear log stack
Log::Log4perl::NDC->remove();
Log::Log4perl::NDC->push('load-ssndi:');

# switch to data directory
chdir($DATA_DIR) or $L->logdie("unable to chdir to $DATA_DIR, exiting");

GetOptions("verbose+" => \$OPT_verbose,
           "md5=s" => \$OPT_md5,
           "commit=s" => \$OPT_commit,
           "update" => \$OPT_update,
           "database=s" => \$OPT_database,
           "help" => \$OPT_help, ) || die "error reading options";

# can either load local file or download and load update, not both
if ($OPT_help || (@ARGV != 1 && !defined $OPT_update) || ($OPT_update && @ARGV)) {
    &HELP_MESSAGE();
    exit(1)
}

# increase verbosity if we've been asked to do so
if ($OPT_verbose) {
    $L->more_logging($OPT_verbose);
    for my $package (qw/DBIx.Log4perl/) {
        my $log = Log::Log4perl->get_logger($package);
        $log->more_logging($OPT_verbose);
    }
}

# either download update or use supplied file name
if ($OPT_update) {
    $L->info("starting web update of death index");
    # OPT_md5 is set during download
    ($infile, $OPT_md5) = &download_update();
} else {
    $L->info("starting load of local file/update");
    $infile = $ARGV[0];
}

# md5 will always be checked for updates downloaded from the web
if ($OPT_md5) {
    if (!&check_md5($infile,$OPT_md5)) {
        # checksum failed, remove any files on disk and exit
        unlink $infile if (-e $infile);
        my $md5_file = "$infile.md5";
        unlink $md5_file if (-e $md5_file);
        exit(1);
    }
}

&init_db();
&load_db($infile);

$dbh->disconnect;
$L->info("load complete");

############################################ subs ###########################################
sub download_update() {
    # download the monthly update from the NTIS website
    # update files are listed in a table with the following columns:
    # [' File Name ',' Size ',' Date (Y/M/D) ',' Frequency ','Record Count',' MD5 Checksum ']

    # file name prefix for monthly files is 'MA' - (M)onthly (A)scii
    # file name is 'prefixYYMM01': YY is two digit year, MM is two digit month, day is always 01
    my ($update_file,$md5, $md5_file);

    # we'll find the row we want by matching on 'Date Y/M/D' column
    # as above, the day is always 01
    my $date = `date +%Y/%m/01`;
    chomp($date);

    # for wget log
    my $wget_log = "$LOG_DIR/wget-$LOGDATE.log";

    # screen scrape the index page to grab the filename and md5 for the current month
    # if download fails due to cert check, can add --no-check-certificate
    $L->info("slurping $MONTHLY_URL, looking for $date update");
    my $index = `wget -O - -a$wget_log $MONTHLY_URL`;
    $L->logdie("Failed trying to get $MONTHLY_URL, check $wget_log") unless length($index) != 0;

    # the table listing update files is first table inside another table
    my $te = HTML::TableExtract->new(depth => 1, count => 0);
    $te->parse($index);

    foreach my $row ($te->rows) {
        next unless (defined $row->[0]); # skip blank rows
        # we want the ASCII file ('MA' prefix), not the EBCDIC one
        if ($row->[0] =~ m/MA/ && $row->[2] =~ m{$date}) {
            # the table cells have random padding in them
            ($update_file = $row->[0]) =~ s/\s//g;
            ($md5 = $row->[5]) =~ s/\s//g;
            $md5_file = "$update_file.md5";
            $L->info("found filename: $update_file md5: $md5\n");
        }
    }
    $L->logdie("Could not find update file for $date") unless defined $update_file;

    if (-e $update_file) {
        $L->info("found existing update file $update_file on disk, using");
        save_md5($md5_file,$md5) unless (-e $md5_file);
        return ($update_file, $md5);
#        unlink $update_file;
    }

    my $DOWNLOAD_URL .= $DOWNLOAD_URL."/$update_file";
    # TODO: update with your login for the DMF download site
    # move user/pass into wgetrc?
    my ($USER, $PASS) = ('','');
    my @ARGS = ("wget","-a$wget_log","--read-timeout=60","--secure-protocol=auto", "--http-user=$USER","--http-password=$PASS","--tries=5",$DOWNLOAD_URL);

    # TODO: add more checking here, wget is dying silently mid-download
    $L->info("downloading monthly update, saving to file $update_file");
    system(@ARGS) == 0 || $L->logdie("wget failed $?, check $wget_log");

    # save md5 to file in case we have to reload in the future
    save_md5($md5_file,$md5);

    return ($update_file, $md5);
}

sub save_md5() {
    my ($fname, $md5) = @_;
    $L->info("saving md5 *$md5* as $fname");
    open(FOUT, ">", "$fname") or $L->logdie("cannot open $fname for writing");
    print FOUT $md5;
    close(FOUT);
}
sub check_md5() {
    my ($file,$md5) = @_;
    my $data;

    $L->info("checking md5 for $file");
    {
        local($/, *FIN);
        open(FIN, $file) or die "cannot read file $file\n";
        $data = <FIN>;
        close(FIN);
    }
    if (md5_hex($data) eq $md5) {
        $L->info("checksum for $file matches, proceeding");

    } else {
        print "error: checksum for update file does not match, exiting\n";
        return 0;
    }
    return 1;
}

sub load_db() {
    # reads records from file, inserts/updates/deletes from db
    my $file = shift;
    my $rc;
    my $msg;

    $L->info("starting load of $file");

    # disabling keys (indexes) improves speed of load
    # TODO: this does not work for Innodb plugin in 5.1, will work in 5.5
    # $dbh->do("ALTER TABLE death_index DISABLE KEYS");

    open(FIN, $file) or $L->logdie("cannot read file $!");
    while (<FIN>) {
        # see perldoc above for order and description of fields
        my $status = substr($_,0,1);
        my $fields = [substr($_,1,9),substr($_,10,20),substr($_,30,4),
                   substr($_,34,15),substr($_,49,15),substr($_,64,1),
                   substr($_,65,8),substr($_,73,8)];


        # check early that dobirth/dodeath are valid and skip add/update if invalid
        # example of bad dodeath from May 2012 update (MA120501):
        # A279208203ECKLE   FORREST        V              P6000620001241924
        # The date of death is '60-00-6200'
        # dodeath = $fields->[6]
        # dobirth = $fields->[7]
        my ($dod_m,$dod_d,$dod_y) = $fields->[6] =~ /^(\d{2})(\d{2})(\d{4})$/;
        my ($dob_m,$dob_d,$dob_y) = $fields->[7] =~ /^(\d{2})(\d{2})(\d{4})$/;
        if (!check_date($dod_y,$dod_m,$dod_d) || !check_date($dob_y,$dob_m,$dob_d)) {
            $L->warn("$fields->[0]: invalid date(s) dob $fields->[7] dodeath $fields->[6] skipping");
            next;
        }

        for (my $i = 0; $i <= 7;$i++) {
            # bind params are numbered from 1
            $insert_sth->bind_param($i+1,$fields->[$i]);
        }

        # TODO: use extended INSERT syntax with multiple VALUES clauses?
        # TODO: create csv file for full loads? (may not be any faster)

        eval {
            # sneaky trick to break out of the if statements
            for (;;) {
                # see perldoc above for meaning of status flags
                if ($status eq ' ' || $status eq 'A') {
                    $msg = "Adding $fields->[0]";
                    $rc = $insert_sth->execute();
                    $L->info("$msg...row added") if (1 == $rc);
                    # ON DUPLICATE...should return 2, but instead returns 3
                    # seems to be due to the timestamp (NOW()) in update clause
                    $L->warn("$msg...row updated instead") if (3 == $rc || 2 == $rc);
                    last;
                } elsif ($status eq 'C') {
                    $msg = "Updating $fields->[0]";
                    $rc = $insert_sth->execute();
                    $L->warn("$msg...row added instead") if (1 == $rc);
                    $L->info("$msg...row updated") if (3 == $rc || 2 == $rc);
                    last;
                } elsif ($status eq 'D') {
                    $msg = "Deleting $fields->[0]";
                    $delete_sth->bind_param(1,$fields->[0]);
                    $rc = $delete_sth->execute();
                    $L->info("$msg...row deleted") if (1 == $rc);
                    last;
                } else {
                    $L->error("Bad status $status for $fields->[0]");
                }
            }

            $L->warn("$msg...no row found") if ('0E0' eq $rc);
            $L->error("$msg...unknown rows affected") if (-1 == $rc);


            # commit every so often
            if (0 == $. % $OPT_commit) {
                $L->info("$. rows processed, commit");
                $dbh->commit;
            }
            undef ($msg);
        };

        if ($@) {
            $L->fatal("Transaction aborted due to !@, rolling back");
            eval { $dbh->rollback};
            # TODO: do we really want to die here vs continuing and
            #       logging the bad transactions?
            close(FIN);
            $dbh->disconnect;
            exit(1);
        }
    }
    # final commit to catch stragglers
    $L->info("finished processing $. rows, final commit");
    $dbh->commit;

    # Now re-enable keys to update the indexes
    #$L->info("Load complete, updating indexes");
    #$dbh->do("ALTER TABLE death_index ENABLE KEYS");
    #$L->info("Indexing complete");

    close(FIN);
}

sub init_db() {
    # opens database connection and inits statement handles
    $L->info("Using database $OPT_database on host $OPT_dbhost");
    $L->info("Commiting every $OPT_commit rows") if ($OPT_commit);
    $dbh = DBIx::Log4perl->connect($CONNECT_STRING, $OPT_dbuser, $OPT_dbpass,
                                    { RaiseError => 1, AutoCommit => 0 })
        or die "Can't connect to $CONNECT_STRING as user $OPT_dbuser: $DBI::errstr\n";

    # use the same insert statement for both (A) and (C) records since the
    # record layout spec says we should add/update for both
    # also use for full file (status eq ' ') records.
    $insert_sth = $dbh->prepare('INSERT INTO death_index '.
                               '(ssn,last,suffix,first,middle,verified,dodeath,dobirth,'.
                               'created,updated) '.
                               'VALUES(?,?,?,?,?,?,'.
                               "STR_TO_DATE(?,'%m%d%Y'),STR_TO_DATE(?,'%m%d%Y'),NOW(),NOW()) ".
                               "ON DUPLICATE KEY UPDATE last=VALUES(last),".
                               'suffix=VALUES(suffix), first=VALUES(first),middle=VALUES(middle),'.
                               'verified=VALUES(verified),'.
                               'dodeath=VALUES(dodeath),dobirth=VALUES(dobirth),updated=NOW()');
    $delete_sth = $dbh->prepare('DELETE FROM death_index WHERE ssn=?');
}

sub HELP_MESSAGE() {
    print <<EOM;
Usage: $0 [options] (<file>)
load file of ssn death index data into data warehouse. Two modes:
1) If <file> given, loads <file>. Used for full file load or to apply an historical update.
2) If -u option given, downloads and applies current monthly update from NTIS website.
    Meant to be used when called from cron. Incompatible with <file>

Options:
    -v, --verbose\t:(may specify multiple) raise debug level
    -u, --update\t:run in update mode. Implies --md5. Do not specify <file>
    --md5=MD5_HEX\t:verify integrity of monthly update file against given MD5.
    -c, --commit=N\t:commit interval, default is 100 rows
    -d, --database=DB\t:change target database. default is 'ssn'
EOM
}

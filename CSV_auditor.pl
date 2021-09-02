#   CSV_auditor.pl
#
#   24-Aug-2021 ->readAndAuditCSV()
#   23-Aug-2021 
#       Add CL switch to identify the record ID column [0] is default.
#   10-Aug-2021
#       was saved as tag v1.0
#       Additions to support DSL >= or 'ge' <value>, factor (could be -1)
#   02-Aug-2021
#       Change to CL switches. -o <statsReport> will be -r <statsReport
#                              -o <output> will be the auditLog output
#   07-Jul-2021
#   20-Jul-2021
#
use strict;
use lib "./";
use MAPPER;
use Data::Dumper;
use Text::CSV;
use Getopt::Std;
###############################
# /mnt/hgfs/ShareToVM/Unicauca-dataset-April-June-2019-Network-flows.csv
###############################
my %opts;
getopts('l:o:s:k:m:c:r:d:i:',\%opts);
#print %opts;
my $nl="\n";
my $inCSV = $opts{'l'};
my $skip_first_row = $opts{'k'};            # required 0 or 1
my $maxRows=100;
$maxRows = $opts{'m'} if (defined $opts{'m'} );
my $schemaConversionCSV = $opts{'s'};
my $recordIDcolumn=0;                                   # col assumed to contain the record ID
$recordIDcolumn = $opts{'i'} if (defined $opts{'i'});    # override if provided
####
usage();
die "Please provide -k 0 (first record is data) or 1 (first record is header)" if (!defined $opts{'k'});
die "Please provide -s SchemaConversionFileCSV " if (!defined $opts{'s'});
#   prior generated stats report 
die "Please provide -r <stats_report> " if (!defined $opts{'r'});
die "Please provide -o <outputAuditReportName> " if (!defined $opts{'o'});
####
sub usage
{
    my $usage = 'perl CSV_auditor.pl -l <inputCSV> -s <SchemaConversionFileCSV> -r <outStatsReport> -m <maxrows> -k <0=first row is data; 1=skipFirstRowAsHeader> -c "colNum:opCode:opVal," -i <recordIDcolumn>  ';
    printf("%sUsage:$nl %s $nl",$nl,$usage);
}
####
my $ma = MAPPER->new();

#   read the conversion schema CSV
$ma->readConversionSchema($schemaConversionCSV);

#   list column names / types
$ma->writeOutColumnNamesTypes();

#   read a prior-generated stats report, info for each column
$ma->read_stats_report($opts{'r'});     # prior output report file, read and parse to object

#   -c '<audit clauses separated by comma, surrounded by quotes>'
my @colsToShow = split(',',$opts{'c'});     # colNum:opCode:opVal, ALSO will be cols to Audit

#   -l <list of CSV files to audit>
my @fileList = split(',',$inCSV);           # input dataset csv files to read

#   -d <description for the test clauses in -c>
my $testClauseDesc = $opts{'d'};     # dsl:description

#   -o <outputAuditReportName>     # text file for audit records
my $auditReportName = $opts{'o'};

#   call the reader and audit
$ma->readAndAuditCSV(\@fileList,\@colsToShow,$maxRows,$skip_first_row,$auditReportName,$testClauseDesc,$recordIDcolumn);     
            # later, this will call readAndAuditCSV()

print "Time unix is: ",time(),$nl;


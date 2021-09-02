#   CSV_stats_ranges.pl
#   added to git ECS master v1.0 on 09-Aug-2021
#
#   10-Aug-2021
#       Updated MAPPER to calculate bin statistics. tag v1.1
#       /mnt/hgfs/ShareToVM/Unicauca-dataset-April-June-2019-Network-flows.csv
#       13:26  tag v1.3
#
#   02-Aug-2021
#       Change to CL switches. -o <statsReport> will be -r <statsReport
#                              -o <output> will be the auditLog output
#   02-Jul-2021     (IDS_Ubuntu16)
#   19-Jul-2021   support for 'portRange'
#
#   Using MAPPER (schema mapping tools)
#   Read a CSV, referencing a conversion mapper .csv file (KaggleCSV_to_ECSjson.csv)
#   Assemble statistics/ranges for each column
#   Generate metadata report
#
use strict;
use lib "./";
use MAPPER;
use Data::Dumper;
use Getopt::Std;
####
#   perl CSV_stats_ranges.pl
#   -l  list of CSV input data set files
#   -r  output report for statistics of the file collection
#   -s  Schema csv with fields and mapping
#   -k  skip 1st, because it is a header row
#
####
my $nl="\n";
my %opts;
getopts('l:r:s:k:m:',\%opts);
#print %opts;
my $inCSV = $opts{'l'};
my $outputReportFileName = $opts{'r'};
# my $skip_first_row=0;
my $skip_first_row = $opts{'k'};            # required 0 or 1
my $maxRows = 1000;      # num of records of csv to process
$maxRows = $opts{'m'} if (defined $opts{'m'} );
my $schemaConversionCSV = $opts{'s'};
usage();
die "Please provide -k 0 (first record is data) or 1 (first record is header)" if (!defined $opts{'k'});
die "Please provide -s SchemaConversionFileCSV "    if (!defined $opts{'s'});
die "Please provide -r outputReportFileName "       if (!defined $opts{'r'});
die "Please provide -l <list of data set csv files> " if (!defined $opts{'l'});
####
sub usage
{
    my $usage = 'Usage: 
        perl CSV_stats_ranges.pl -l <inputCSV> -s <SchemaConversionFileCSV> -r <outStatsReport> -m <maxrows> -k <1=skipFirstRowAsHeader>';
    print $usage,$nl;
}
############################
# ShareToVM Unicauca-dataset-April-June-2019-Network-flows.csv
############################
my $ma=MAPPER->new();

##my $conversionSchemaFile = "KaggleCSV_to_ECSjson.csv";
$ma->readConversionSchema($schemaConversionCSV);
my @colNames = @{$ma->{_oldFieldNamesAR}};

$ma->{_outputReportFileName} = $outputReportFileName;
print Dumper($ma);

my @fileList = split(',',$inCSV);
$ma->stats_CSV(\@fileList,\@colNames,$maxRows,$skip_first_row);
$ma->generate_report();


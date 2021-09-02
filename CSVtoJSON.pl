#	CSVtoJSON.pl  (VM_IDS)
#
#   24-Aug-2021 Updated calling arguments, new @colNames from object
#   29-Jun-2021
#   16-Jun-2021
#   Uses MAPPER object/methods.
#s
#   Reads a csv file. Reads a header and specification for the csv
#   As it reads the csv, audit the fields for compliance with specification
#   Label any non-compliant, non-complete records
#   If given a conversion schema/specification, then re-write each record
#   in a stream mode. 
#   Optional: generate a report of distribution of each field.
#
use lib "./";
use MAPPER;
use strict;
use Data::Dumper;
use Getopt::Std;
####
my $nl = "\n";
my %opts;
getopts('l:j:s:k:m:',\%opts);
####
sub usage
{
    print" Usage: perl CSVtoJSON.pl -s <path/SchemaMappingFile.csv> -l <path/inputFile.csv> 
                          -j <path/outputFile.json> -m <maxRows> -k <0|1> $nl $nl";
}   
####
my $inCSV;
if (defined $opts{'l'})
{
    $inCSV = $opts{'l'};
}   else {
    print "Need -l <path/inputCSV.csv> $nl $nl";
    usage();
    die;
}
my $c2j = MAPPER->new();     # new storage object, with inverted index
my $nl="\n";
my $count;
my $max = 100;
$max = %opts{'m'} if (defined $opts{'m'});
#############################################
# "Unicauca2019_schemaMap.csv"
my $conversionSchemaFile;
if (defined $opts{'s'})
{
    $conversionSchemaFile = $opts{'s'} 
}   else {
    print "Need -s <path/conversionSchemaFile.csv> $nl";
    usage();
    die;
}    
$c2j->readConversionSchema($conversionSchemaFile);
my @colNames = @{$c2j->{_newFieldNamesAR}};

print Dumper(\@colNames);
#print Dumper($c2j);

my $jsonFileName;
if (defined $opts{'j'})
{
    $jsonFileName = $opts{'j'};
} else {
    print "Need -j <path/outputJSON.json> $nl $nl";
    usage();
    die;
}
#
print "Converting obj to JSON and output to $jsonFileName $nl";
$c2j->readCSV($inCSV,\@colNames,$jsonFileName,$max);

print "Done. json: $jsonFileName $nl";




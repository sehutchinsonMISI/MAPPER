#   AT_for_MAPPER.pl
#
#   24-Aug-2021 epoch time, and year2030 tests
#   23-Aug-2021 CL switch for -i <recordIDcolumn> and -d <testXXXDesc>
#   02-Aug-2021
#       Change to CL switches. -o <statsReport> will be -r <statsReport
#                              -o <output> will be the auditLog output
#   Really, AT_for_MAPPER.pl
#   29-Jul-2021
#
#   Acceptance tests based on Unicauca2019 excerpt.
#
package ATs;
use strict;
use Data::Dumper;
use Getopt::Std;
####
my $nl="\n";
my $maxRows=2;
my %opts;
getopts('m:i:',\%opts);
if (!defined $opts{'m'})
{
    die "Usage: perl AT_for_MAPPER.pl -m <numberOfRows> -i <recordIDcolumn>" ;
} else {
    $maxRows = $opts{'m'};
}
my $recordIDcolumn=0;       # default
$recordIDcolumn = $opts{'i'} if (defined $opts{'i'});
####
sub new
{
    my $class=shift;
    my $self={
    
    };
    bless $self,$class;
    return $self;
}
####
sub run_tests
{
    my $self=shift;
    #
    my $prog                = $self->{_perlProg};           #   = "CSV_auditor.pl";
    my $schemaMap           = $self->{_schemaMap};          #   = "KaggleCSV_to_ECSjson.csv";
    my $atData              = $self->{_atData};             #   = "UnitTestingData.csv";
    my $statsReport         = $self->{_statsReport};        #   = "Unicauca2019_stats.txt";
    ## my $testClause          = $self->{_testClauses}->[0]->[0];  #   = [
    ## my $testDescription     = $self->{_testClauses}->[0]->[1];  # the description for this clause
    my $testOutputReportBase    = $self->{_outputATreport_base};
    #
    print "The clauses AR: ",Dumper($self->{_testClauses}),$nl;
    #
    #   the sequence
    my $testNum = 0;
    my $allClauses;
    my $allDescriptions;
    #
    foreach my $testAR ( @{$self->{_testClauses}} )
    {        
        $allClauses .= $testAR->[0] . ',' ;
        $allDescriptions .= $testAR->[1] . ',';       
    }
    #
    print "testAR: ",Dumper($allClauses),$nl;   
    #    
    my $thisTest = $testOutputReportBase . '_' . $testNum . '.out';
    my $thisLog  = $testOutputReportBase . '_' . $testNum . '.log';
    #
    print "RECORDidCOLUMN is: $recordIDcolumn $nl";
    #
    printf("command: perl %s -s %s -l %s -r %s -c '%s' -o %s -k 0 -m $maxRows -i $recordIDcolumn > %s $nl",
        $prog,
        $schemaMap,
        $atData,
        $statsReport,
        $allClauses,
        $thisTest,
        $thisLog,
        );
    my $cmd = sprintf("perl %s -s %s -l %s -r %s -c '%s' -o %s -k 0 -m $maxRows -i $recordIDcolumn > %s $nl",
        $prog,
        $schemaMap,
        $atData,
        $statsReport,
        $allClauses,
        $thisTest,
        $thisLog,
        );
    print "cmd: $cmd "; 
    #   here, we actually execute the cmd and gather result.
    #   result not used yet.
    my $result = ` $cmd `;  # execute the command
    #
    $testNum += 1; 
}
####
sub setup_tests
{
    my $self=shift;
    $self->{_perlProg}      = "CSV_auditor.pl";
    $self->{_schemaMap}     = "tests/Unicauca2019_schemaMap.csv";
    $self->{_atData}        = "tests/UnitTestingData.csv";
    $self->{_statsReport}   = "tests/stats_ranges_report.txt";
    $self->{_testClauses}   = [
        ['7:NotIn:histo:2','col7 val NotIn bin_2'],
        ['7:IsIn:histo:2','col7 val IsIn bin_2'],
        ['3:>:dmin:D', 'col3 > dmin of Distribution'],
        ['3:<:dmax:D', 'col3 < dmax of Distribution'],
        ['3:=:dmean:D','col3 = dmean of Distribution'],
        ['8:NotIn:histo:2','col7 val NotIn bin_2'],
         ['1:<:min:0',  'col1 < min of bin_0:'],
         ['1:>:min:1',  'col1 > min of bin_1:'],
         ['1:<:min:2',  'col1 < min of bin_2:'],
         ['1:=:min:3',  'col1 = min of bin_3:'],
         ['1:<:max:0',  'col1 <> max of bin_0:'],
         ['1:>:max:1',  'col1 > max of bin_1:'],
         ['1:<:max:2',  'col1 < max of bin_2:'],
         ['1:=:max:3',  'col1 = max of bin_3:'],
         ['1:<:mean:0',  'col1 < mean of bin_0:'],
         ['1:>:mean:1',  'col1 > mean of bin_1:'],
         ['1:<:mean:2',  'col1 < mean of bin_2:'],
         ['1:=:mean:3',  'col1 = mean of bin_3:'],
         ['3:<:value:49175', 'col3 < value 49175'],
         ['3:>:value:49175', 'col3 > value 49175'],
         ['3:=:value:49175', 'col3 = value 49175'],
         ['13:>:epochtime:1555966971', 'col13 > epochtime 1555966971'],
         ['13:<:year2030:2030', 'col13 < epochtime for 2030'],
        ];
    $self->{_outputATreport_base} = "./tests/AT_output_report";
}
####
# col:test{>,<,=,NotIn,IsIn}:statistic{mean,min,max,time,histo}:factorOrBinNum
##################################
my $ats = ATs->new();
$ats->setup_tests();
#
my ($result,$testDescription) = $ats->run_tests();
printf("Result: %s Description: %s $nl",$result,$testDescription);




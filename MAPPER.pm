#	MAPPER.pm  (IDS_Ubuntu16 vm)
#
#   24-Aug-2021 'readAndAuditCSV', EpochTime, Kaggle2017 time, ISO8601
#   23-Aug-2021 support for audit -i <recordIDcolumn> override
#   18-Aug-2021 16:30 - added parsing for dd/mm/yyyy HH:MM:SS
#   10-Aug-2021 09:15
#   09-Aug-2021 added to git ECS master v1.0 on 09-Aug-2021
#
#   Generate bin statistics. commit as tag v1.1
#   13:26  tag v1.3 
#   17:40  correct outputStatsReport to have | $key : <values> |  for uniform parsing.
#                   {bin_0,bin_1,bin_2,bin_3}:<[num,min,max,mean]>
#   02-Aug-2021
#       Change to CL switches. -o <statsReport> will be -r <statsReport
#                              -o <output> will be the auditLog output
#   This module has methods of 2 kinds. (1) reading the schemaMappingCSV, then 
#       reading the source CSV (up to max lines) generating stats_ranges
#       emitting an output statsReport.
#
#   (2) auditing - first read the schemaMappingCSV, and the statsReport, then
#       re-reading the source CSV (up to max lines) and emit audit records
#
#   Reads a csv file. Reads a header and specification for the csv
#   (1) summarize statistics for each column. Generate output report
#   (2) As it reads the csv, audit the fields for compliance with specification
#   (future) Label any non-compliant, non-complete records
#   If given a conversion schema/specification, then re-write each record
#   in a stream mode. 
#
package MAPPER;
#use Storable;
use strict;
use Data::Dumper;
use JSON;
use Text::CSV;
use Text::CSV_PP;
use DateTime;
####
my $count;
my $nl="\n";
my $csv = Text::CSV->new( {sep_char => ',' , allow_loose_quotes => 1 });
my $JSON = JSON->new->utf8;
$JSON->convert_blessed(1);
$JSON->allow_blessed(1);
my $OBJECT;
my $convOBJECT;
my $log = 0;        # set to == 1 for log printing to console
my $logRecords = 1; # show incoming record
####
sub new
{
    my $class=shift;
    my $self= {
    
    };
    bless $self,$class;
    return $self;
}
####
sub writeJSONtoFile
{
    my $self=shift;         # $j->JSONtoFile($pobj);
    my $perlObject=shift;
    my $jfhandle=shift;
    #
    my $json = $JSON->encode($perlObject);
    #print Dumper($json);
    printf $jfhandle ("%s $nl",$json);
    
}
####
#   this is the main read and process entry for CSV_stats_ranges.pl
#
sub stats_CSV
{
    my $self=shift;
    my $fileList=shift;
    my $colNamesAR=shift;
    my $maxRows=shift;
    my $skip_first_row=shift;
    #
    foreach my $csvFile ( @{$fileList} )
    {
        print STDERR "Now reading file: $csvFile $nl";
        $self->{_lastCSV} = $csvFile;
        $self->{_filesRead} .= $csvFile . ',' ;
        #
        $count=0;       # reset the counter
        open(my $data, '<', $csvFile) or die "Could not open $csvFile";
        print "Now reading file: $csvFile $nl";
        
        my $first_record = <$data> if ($skip_first_row);
        
        while (my $line = <$data>)
        {
	        if ($csv->parse($line))
	        {
	            $count += 1;
	            print STDERR "$csvFile $count\n" if ($count % 10000 == 0);
	            
	            $self->{_globalRecordCount} += 1;     # across all files, total records
	            
		        my @fields = $csv->fields();
		        print "Parsing record $count $nl" if ($log);

		        $self->stat_this_record(\@fields,$colNamesAR);
		        #
		        if ($count >= $maxRows)
		        {
		            $self->generate_report();
		            print "-- Stopped after $maxRows rows. Set larger using -m <number> $nl";
		            die "-- Only $maxRows were processed --";
		        }	
	        }
        }
    } 
}    
####
#   MAPPER::stats_CSV reads the next line and calls ::stat_this_record
#       passing in the fieldvalues in fieldsAR, and old colNamesAR.
#   stat_this_record then calls ::do_stats on each field/value
####
sub stat_this_record
{
    my $self=shift;
    my $fieldsAR=shift;
    my $colNamesAR=shift;
    my $fNum=0;             # counter for field position to index into Arefs
    #
    foreach my $f (@{$fieldsAR})
    {
        printf("  [%d] [%s] [%s] == %s $nl",$fNum,$colNamesAR->[$fNum],
            $self->{_oldDataTypesAR}->[$fNum] , $f) if ($log);
        #
        #   do stats
        #    
        $self->do_stats($fNum,$f,$colNamesAR);
        #
        $fNum += 1;
    }
}
####
#   called by ::stat_this_record
#   has the fNum, fVal, and colNamesAR
#   Performs stats given the oldDataTypesAR for that field
#
####
sub do_stats
{
    my $self=shift;
    my $fNum=shift;
    my $fVal=shift;
    my $colNamesAR=shift;
    #
    my $dType=$self->{_oldDataTypesAR}->[$fNum];
    #case:
    if ($dType eq 'int')
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        $self->{_fieldStats}->{$fNum}->{_sum}   += $fVal;
        $self->{_fieldStats}->{$fNum}->{_mean}  = 
            $self->{_fieldStats}->{$fNum}->{_sum} / 
            $self->{_fieldStats}->{$fNum}->{_recs};
        # set first max and min to the current value
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if ($fVal > $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if ($fVal < $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->do_histo($fNum, $fVal, [65536, 16777216, 4294967296]);
        return;
    };
    #
    if ($dType eq 'float')
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        $self->{_fieldStats}->{$fNum}->{_sum}   += $fVal;
        $self->{_fieldStats}->{$fNum}->{_mean}  = 
            $self->{_fieldStats}->{$fNum}->{_sum} / 
            $self->{_fieldStats}->{$fNum}->{_recs};
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if ($fVal > $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if ($fVal < $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->do_histo($fNum, $fVal, [65536, 16777216, 4294967296]);
        return;    
    };
    #
    if ($dType eq 'string')
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        #
        my $firstChar = substr($fVal,0,1);
        #print "Sending firstChar $firstChar $nl";
        $self->do_histo($fNum, ord(uc($firstChar)), [48,58,65]);
        return;    
    };
    #
    if ($dType eq 'Ipstring')
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if ($fVal gt $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if ($fVal lt $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->{_fieldStats}->{$fNum}->{_nonRoutables} += 1 
            if ($fVal =~ /^192\.168\./ || $fVal =~ /^10\./ || $fVal =~ /^172\.1[6-9]\./ || 
            $fVal =~ /^172\.2[0-9]\./ || $fVal =~ /^172\.31\./ 
            );
        #
        my @octets = split(/\./,$fVal);
        my $firstOctetVal = int($octets[0]);
        #print "Sending firstOctetVal = $firstOctetVal $nl";
        $self->do_histo($fNum, $firstOctetVal, [63,127,191]);
        return;    
    };
    #
    if ($dType eq 'Portnum')
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        $self->{_fieldStats}->{$fNum}->{_sum}   += $fVal;
        $self->{_fieldStats}->{$fNum}->{_mean}  = 
        $self->{_fieldStats}->{$fNum}->{_sum} / 
        $self->{_fieldStats}->{$fNum}->{_recs};
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if ($fVal > $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if ($fVal < $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->{_fieldStats}->{$fNum}->{_wellKnown} += 1 
            if ($fVal >= 0 && $fVal < 1024);
        #    
        $self->{_fieldStats}->{$fNum}->{_registered} += 1 
            if ($fVal >= 1024 && $fVal < 49151);
        #    
        $self->{_fieldStats}->{$fNum}->{_ephemeral} += 1 
            if ($fVal > 49151);
        #

        $self->do_histo($fNum, $fVal, [1024,49151,65535]);
        return;    
    };
    #
    if ($dType eq 'unix_fractional')
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $fVal if ($fVal > $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $fVal if ($fVal < $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->do_histo($fNum, $fVal, [65536, 16777216, 4294967296]);
        return;    
    };
    #
    if ($dType eq 'dd/mm/yyyyHH:MM:SS')     # for Unicauca2017 / Kaggle data set
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        my $msVal = $self->convertToMS($fVal);
        $self->{_fieldStats}->{$fNum}->{_max} = $msVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $msVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $msVal if ($msVal > $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $msVal if ($msVal < $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->do_histo($fNum, $fVal, [65536, 16777216, 4294967296]);
        return;    
    };
    #
    if ($dType eq 'yyyy/mm/ddThh:mm:ss')     # for Unicauca2017 / Kaggle data set
    {
        $self->{_fieldStats}->{$fNum}->{_oldField} = $colNamesAR->[$fNum];
        $self->{_fieldStats}->{$fNum}->{_recs}  += 1;
        my $msVal = $self->convertToMS($fVal);
        $self->{_fieldStats}->{$fNum}->{_max} = $msVal if (!defined $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $msVal if (!defined $self->{_fieldStats}->{$fNum}->{_min});    
        $self->{_fieldStats}->{$fNum}->{_max} = $msVal if ($msVal > $self->{_fieldStats}->{$fNum}->{_max});
        $self->{_fieldStats}->{$fNum}->{_min} = $msVal if ($msVal < $self->{_fieldStats}->{$fNum}->{_min});
        #
        $self->do_histo($fNum, $fVal, [65536, 16777216, 4294967296]);
        return;    
    };
    #default:
    print "Unknown data type for this field $fNum $nl";
}
####
sub convertToMS
{
    my $self=shift;
    my $inVal=shift;    # dd/mm/yy HH:MM:SS
    # dd/mm/yyyyHH:MM:SS
    my $dmy = substr($inVal,0,10);
    my ($d,$m,$y) = split('/',$dmy);
    my $hms = substr($inVal,10,8);
    my ($h,$m,$s) = split(':',$hms);
    #
    #   may call this msSlashTtime
    #
    my $timeReturned = sprintf("%04s/%02s/%02sT%02s:%02s:%02s",$y,$m,$d,$h,$m,$s);
    #
    return $timeReturned;
}
####
sub convertKaggle2017ToMS  # is RFC  and ISO 8601
{
    my $self=shift;
    my $inVal=shift;    # yyyy/mm/ddThh:mm:ss
    # yyyy/mm/ddThh:mm:ss
    my $dmy = substr($inVal,0,10);
    my ($y,$m,$d) = split('/',$dmy);
    my $hms = substr($inVal,10,8);
    my ($h,$m,$s) = split(':',$hms);
    #
    #   may call this msSlashTtime
    #
    my $timeReturned = sprintf("%04s/%02s/%02sT%02s:%02s:%02s",$y,$m,$d,$h,$m,$s);
    #
    return $timeReturned;
}
####
sub Kaggle2017ToEpoch
{
    my $self=shift;
    my $inVal=shift;    # yyyy/mm/ddThh:mm:ss
    # yyyy/mm/ddThh:mm:ss
    my $dmy = substr($inVal,0,10);
    my ($y,$m,$d) = split('/',$dmy);
    my $hms = substr($inVal,10,8);
    my ($h,$m,$s) = split(':',$hms);
    #
    #   may call this slashTtime
    #
    my $slashTtime = sprintf("%04s/%02s/%02sT%02s:%02s:%02s",$y,$m,$d,$h,$m,$s);
    #
    my $dt = DateTime->new(
        year       => $y,
        month      => $m,
        day        => $d,
        hour       => $h,
        minute     => $m,
        second     => $s,
        nanosecond => 0,
        time_zone  => 'America/Los_Angeles',
    );
    #
    printf("To epoch: %s $nl",$dt->epoch());
    $self->{_recordEpochTime} = $dt->epoch();
    
    #
    return $self->{_recordEpochTime};
}
####
####
#   summarize the data seen, by column, the _stats report.
#   Updated 19-Aug to make keys mutex. 'dmin|dmax|dmean|value|histo|min|max|mean|time'
sub generate_report
{
    my $self=shift;
    
    my $reportFileName=$self->{_outputReportFileName}; 
    #
    open my $RF, ">", $reportFileName;
    print "Opened report file $reportFileName for output $nl";
    #
    my $now_time = localtime;
    # write to output file:
    printf $RF ("# SchemaMappingFile: [%s] | | | $nl",$self->{_schemaMappingFileCSV} );
    printf $RF ("# ReportFileName: [%s] | | | $nl",$reportFileName );
    printf $RF ("# GenerationDate: [%s] | | | $nl",$now_time );
    printf $RF ("# Last csvFile: [%s] | | | $nl",$self->{_lastCSV} );
    printf $RF ("# FileList: [%s] | | | $nl",$self->{_filesRead} );
    printf $RF ("# GlobalRecordCount: [%d] | | | $nl",$self->{_globalRecordCount} );
    #    
    #   all field numbers, $fNum
    foreach my $fNum ( sort {$a <=> $b} keys %{$self->{_fieldStats}} )
    {
        printf("Column statistics for col/field: [%d] $nl",$fNum );
        # oldField
        printf(" oldField: [%s] |",$self->{_oldFieldNamesAR}->[$fNum] );
        # datatype
        printf(" oldDtype: [%s] |",$self->{_oldDataTypesAR}->[$fNum] );
        # newField
        printf(" newField: [%s] |",$self->{_newFieldNamesAR}->[$fNum] );
        # newdatatype
        printf(" newDtype: [%s] |",$self->{_newDataTypesAR}->[$fNum] );
        # min
        printf(" dmin: [%s] |",$self->{_fieldStats}->{$fNum}->{_min} );
        # max
        printf(" dmax: [%s] |",$self->{_fieldStats}->{$fNum}->{_max} );
        # avg
        printf(" dmean: [%s] |",$self->{_fieldStats}->{$fNum}->{_mean} );
        # histo 0,1,2 dividing vals with bin counts either side of divider
        printf(" histo: [%s] <%s< [%s] <%s< [%s] <%s< [%s] |",
            $self->{_fieldStats}->{$fNum}->{_histo}->[0],
            $self->{_fieldStats}->{$fNum}->{_histo_bins}->[0],
            $self->{_fieldStats}->{$fNum}->{_histo}->[1],
            $self->{_fieldStats}->{$fNum}->{_histo_bins}->[1],
            $self->{_fieldStats}->{$fNum}->{_histo}->[2],
            $self->{_fieldStats}->{$fNum}->{_histo_bins}->[2],
            $self->{_fieldStats}->{$fNum}->{_histo}->[3],
            );
        print $nl;
        #
        #   NOW Write out to file:
        #
        printf $RF ("col: [%d] |",$fNum );
        # oldField
        printf $RF (" oldField: [%s] |",$self->{_oldFieldNamesAR}->[$fNum] );
        # datatype
        printf $RF (" oldDtype: [%s] |",$self->{_oldDataTypesAR}->[$fNum] );
        # newField
        printf $RF (" newField: [%s] |",$self->{_newFieldNamesAR}->[$fNum] );
        # newdatatype
        printf $RF (" newDtype: [%s] |",$self->{_newDataTypesAR}->[$fNum] );
        # min
        printf $RF (" dmin: [%s] |",$self->{_fieldStats}->{$fNum}->{_min} );
        # max
        printf $RF (" dmax: [%s] |",$self->{_fieldStats}->{$fNum}->{_max} );
        # avg
        printf $RF (" dmean: [%s] |",$self->{_fieldStats}->{$fNum}->{_mean} );
        # histo 0,1,2 dividing vals with bin counts either side of divider
        printf $RF (" histo: [%s] <%s< [%s] <%s< [%s] <%s< [%s] |",
            $self->{_fieldStats}->{$fNum}->{_histo}->[0],
            $self->{_fieldStats}->{$fNum}->{_histo_bins}->[0],
            $self->{_fieldStats}->{$fNum}->{_histo}->[1],
            $self->{_fieldStats}->{$fNum}->{_histo_bins}->[1],
            $self->{_fieldStats}->{$fNum}->{_histo}->[2],
            $self->{_fieldStats}->{$fNum}->{_histo_bins}->[2],
            $self->{_fieldStats}->{$fNum}->{_histo}->[3],
            );
        #   now, bin stats
        my $bin_0_mean = 0;
        $bin_0_mean = $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_sum} / $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_recs} if ($self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_recs} > 0);
        printf $RF (" bin_0: [%s,%s,%s,%s] |",
            $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_recs},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_min},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_max},
            $bin_0_mean, 
            );
        #
        my $bin_1_mean = 0;
        $bin_1_mean = $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_sum} / $self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_recs} if ($self->{_fieldStats}->{$fNum}->{_bin}->{'0'}->{_recs} > 0);
        printf $RF (" bin_1: [%s,%s,%s,%s] |",
            $self->{_fieldStats}->{$fNum}->{_bin}->{'1'}->{_recs},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'1'}->{_min},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'1'}->{_max},
            $bin_1_mean, 
            );
            #
            my $bin_2_mean = 0;
        $bin_2_mean = $self->{_fieldStats}->{$fNum}->{_bin}->{'2'}->{_sum} / $self->{_fieldStats}->{$fNum}->{_bin}->{'2'}->{_recs} if ($self->{_fieldStats}->{$fNum}->{_bin}->{'2'}->{_recs} > 0);
        printf $RF (" bin_2: [%s,%s,%s,%s] |",
            $self->{_fieldStats}->{$fNum}->{_bin}->{'2'}->{_recs},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'2'}->{_min},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'2'}->{_max},
            $bin_2_mean, 
            );
            #
            my $bin_3_mean = 0;
        $bin_3_mean = $self->{_fieldStats}->{$fNum}->{_bin}->{'3'}->{_sum} / $self->{_fieldStats}->{$fNum}->{_bin}->{'3'}->{_recs} if ($self->{_fieldStats}->{$fNum}->{_bin}->{'3'}->{_recs} > 0);
        printf $RF (" bin_3: [%s,%s,%s,%s] |",
            $self->{_fieldStats}->{$fNum}->{_bin}->{'3'}->{_recs},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'3'}->{_min},
            $self->{_fieldStats}->{$fNum}->{_bin}->{'3'}->{_max},
            $bin_3_mean, 
            );    
        #
        print  $RF $nl;
    }
    print "Completed report file $reportFileName for output $nl";
    #
    #   debug: examine histo-bin-stats
    #
    ###print "DEBUG: histobinstats ",Dumper($self->{_fieldStats});
}
####
#   do_histo_bin_stats
#   Have to do_histo_bin_stats before each return, for min, max, mean each bin
#   Will populate 
#       $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_recs},
#       $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_min},
#       $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_max},
#       $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_mean},
#
####
#commit as tag v1.1
####
sub do_histo_bin_stats
{
    my $self=shift;
    my $binNum=shift;
    my $fNum=shift;
    my $fVal=shift;
    #
    $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_recs}+= 1;
    $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_sum} += $fVal;
    $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_max} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_max});
    $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_min} = $fVal if (!defined $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_min});    
    $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_max} = $fVal if ($fVal > $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_max});
    $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_min} = $fVal if ($fVal < $self->{_fieldStats}->{$fNum}->{_bin}->{$binNum}->{_min});
        
}
####
#   Called by do_stats. Bins them to sep values specified in bins4AR
#   10-Aug-2021: need to add min, max, mean for each bin
#   commit as tag v1.1
####
sub do_histo
{
    my $self=shift;
    my $fNum=shift;
    my $val=shift;
    my $bins4AR=shift;
    #   updates 4-bin histo, using value and designated 3 boundaries [1st, Mid, Top]
    #   Count members in each bin, starting at 0
    if (!defined $self->{_fieldStats}->{$fNum}->{_histo}->[0])
    {
        $self->{_fieldStats}->{$fNum}->{_histo_bins} = $bins4AR;
        $self->{_fieldStats}->{$fNum}->{_histo}->[0] = 0;
        $self->{_fieldStats}->{$fNum}->{_histo}->[1] = 0;
        $self->{_fieldStats}->{$fNum}->{_histo}->[2] = 0;
        $self->{_fieldStats}->{$fNum}->{_histo}->[3] = 0;
    }
    #   Count bin assignments
    #   Have to do_histo_bin_stats before each return, for min, max, mean each bin
    #   Will populate $self->{_fieldStats}->{$fNum}->[$BIN#]->{_min},
    #       $self->{_fieldStats}->{$fNum}->[$BIN#]->{_max},
    #       $self->{_fieldStats}->{$fNum}->[$BIN#]->{_mean},
    #
    if ($val <  $bins4AR->[0])
    {
        $self->{_fieldStats}->{$fNum}->{_histo}->[0] += 1 ; 
        $self->do_histo_bin_stats(0,$fNum,$val);
        return;
    }
    #
    if ($val >= $bins4AR->[0] && $val < $bins4AR->[1])
    {
        $self->{_fieldStats}->{$fNum}->{_histo}->[1] += 1 ;
        $self->do_histo_bin_stats(1,$fNum,$val);
        return;
    }
    #
    if ($val >= $bins4AR->[1] && $val < $bins4AR->[2])
    {
        $self->{_fieldStats}->{$fNum}->{_histo}->[2] += 1 ;
        $self->do_histo_bin_stats(2,$fNum,$val);
        return;
    }
    #    
    if ($val >  $bins4AR->[2])
    {
        $self->{_fieldStats}->{$fNum}->{_histo}->[3] += 1 ;
        $self->do_histo_bin_stats(3,$fNum,$val);
        return;
    }
    # or not valid, so ignore.
}
###########################################
################### auditing methods ########################
###########################################

####
# $ma->JUSTreadCSV(\@fileList,\@colsToShow,$maxRows,$skip_first_row,$auditReportName,$testClauseDesc,$recordIDcolumn)
####
sub readAndAuditCSV             # entrypoint for CSV_auditor.pl
{
    my $self        =shift;
    my $fileList    =shift;
    my $colsToShowAR=shift;
    my $maxRows     =shift;
    my $skip_first_row=shift;
    my $auditReportName=shift;
    my $testClauseDesc =shift;
    my $recordIDcolumn =shift;
    $self->{_recordIDcolumn} = $recordIDcolumn;     # save it
    #
    foreach my $csvFile ( @{$fileList} )
    {
        print STDERR "Now input CSV file: $csvFile $nl";
        $count=0;       # reset the counter
        open(my $data, '<', $csvFile) or die "Could not open $csvFile";
        print "Now input CSV file: $csvFile $nl";
        #   open outputReportFileName
        open(my $orf, '>', $auditReportName) or die "Could not open $auditReportName";
        
        while (my $line = <$data>)
        {
	        if ($csv->parse($line))
	        {
	            $count += 1;
	            print STDERR "$csvFile $count\n" if ($count % 10000 == 0);
	            
		        my @fields = $csv->fields();
		        print "Parsing record $count $nl" if ($log);
		        print "$nl $nl==== New record $count $line $nl" if ($logRecords);
                #
                $self->auditFields(\@fields, $colsToShowAR, $orf, $testClauseDesc);        # testing audit
                #$self->showFields(\@fields, $colsToShowAR);
		        #
		        if ($count >= $maxRows)
		        {
		            print "-------------------- done -------------- $nl";
		            #print Dumper($self);
		            print "MaxRows $maxRows exceeded. Set higher using -m <maxNumberOfRows> $nl";
		            print "Unix time now is: ",time(),$nl;
		            #die "Reached max $count";
		            return;
		        }	
	        }
        }
    } 
} 
####

####
sub parseHistobin
{
    my $self=shift;
    my $cval=shift;
    my $binNum=shift;
    #
    printf("    histobin_%d: %s (count,min,max,mean) $nl",$binNum,$self->{_statsCol}->[$cval]->{"bin_".$binNum});
    #   "count,min,max,mean"
    my ($count,$min,$max,$mean) = split(',',$self->{_statsCol}->[$cval]->{"bin_".$binNum});
    return ($count,$min,$max,$mean);
}
####
sub testSub
{
    my $self=shift;
    my $arg1=shift;
    my $test=shift;
    my $arg2=shift;
    return ($arg1 < $arg2) if ($test eq '<');
    return ($arg1 > $arg2) if ($test eq '>');
    return ($arg1 == $arg2) if ($test eq '=');
    return ($arg1 <= $arg2) if ($test eq '<=');
    return ($arg1 >= $arg2) if ($test eq '>=');
    return ($arg1 != $arg2) if ($test eq '!=');   
}
###################################################################
#   called by JUSTreadCSV to audit those specified in -c <clauses>
#   1/1/2030 is 1893474001
###################################################################
sub auditFields
{
    my $self=shift;
    my $fieldsAR=shift;         # current record
    my $colsToAuditAR=shift;    # cols to audit
    my $auditOutHandle=shift;   # file handle
    my $testClauseDesc=shift;
    #
    #   About histobins:
    #       a bin_0,_1,_2,_3 is populated only if one or more values falls in that bin.
    #       Bin extents are determined by the declared datatype, so
    #       histobin audit ops will be constrained to only operate on values that
    #       fall within THAT bin, and not globally. Pictorially this is:
    #               --------------- bin_x -----------
    #       ........|....!.......!............!.....|.......
    #              sep   min   mean          max    sep
    #
    #   DSL clause structures  for -c <clause>
    #
    ## 4-tuple:     col : test{>,<,=,NotIn,IsIn} : statistic{mean,min,max,time,histo} : factor|bin
    #
    # Datatypes: int, float, Ipstring, recordID, Portnum - all have stats and histogram
    #               col :       test        :    statistic              : parameter
    #                                        <these mutually exclusive>
    # Distribution: col : test{>,ge,<,le,=} : statistic{dmin,dmax,dmean}: {Distribution|D}
    # A_value:      col : test{>,ge,<,le,=} : value                     : <particularNumericValue>
    # DistribBin:   col : test{IsIn,NotIn}  : histo                     : bin# {0,1,2,3}
    # HistoBinStat: col : test{>,ge,<,le,=} : statistic{min, max, mean} : bin# {0,1,2,3}  eg. 'min of bin_1'
    # Timestamp:    col : test{>,<}         : time                      : year {2000,2030} 
    #
    foreach my $colOp ( @{$colsToAuditAR} )  
    {
        my ($cval,$test,$statistic,$parameter) = split(":",$colOp);
        
        print "Audits to perform: $cval,$test,$statistic,$parameter $nl"; 
        
        ####### statistic eq value #######################
        #### parameter is the arg2 value #################
        if ($statistic eq 'value' )
        {
            #              [0]     cval   ->[cval] test par   stat ->{statistic}
            printf("    ID: %s col: %d value: %s is %s the %s (%s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,

                $statistic,
                $parameter) 
                if ( $self->testSub($fieldsAR->[$cval],$test,$parameter ));
                #
            printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (%s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,

                $statistic,
                $parameter) 
                if ( $self->testSub($fieldsAR->[$cval],$test,$parameter ));            
        }
        
        ####### bin_N stats ##############################
        if ($statistic eq 'min' )
        {
            # parse histo to find actual bin_N for this value as $binNum
            my ($binNum, $lowerBinVal, $upperBinVal, $proportion) = $self->parseHisto($cval,$fieldsAR->[$cval]);
            if ($binNum ==$parameter)
            {
                printf("   --> val %s is in bin: %d == testBin: %d $nl",$fieldsAR->[$cval],$binNum,$parameter );
                #   parse the histobin and test the fVal
                my ($count,$min,$max,$mean) = $self->parseHistobin($cval,$binNum);
                #   now that these histobin vals are here, incorporate in the test blocks
                #              [0]     cval   ->[cval] test par   stat ->{statistic}
                printf("    ID: %s col: %d value: %s is %s the %s (of bin_%s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,
                    $statistic,
                    $binNum)
                    if ( $self->testSub($fieldsAR->[$cval],$test,$min) );
                    #
                printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of bin_%s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,
                    $statistic,
                    $binNum) 
                    if ( $self->testSub($fieldsAR->[$cval],$test,$min) );  
            }    
        }
        ####### bin_N stats ##############################
        if ( $statistic eq 'max' )
        {
            # parse histo to find actual bin_N for this value as $binNum
            my ($binNum, $lowerBinVal, $upperBinVal, $proportion) = $self->parseHisto($cval,$fieldsAR->[$cval]);
            if ($binNum ==$parameter)
            {
                printf("   --> val %s is in bin: %d == testBin: %d $nl",$fieldsAR->[$cval],$binNum,$parameter );
                #   parse the histobin and test the fVal
                my ($count,$min,$max,$mean) = $self->parseHistobin($cval,$binNum);
                #   now that these histobin vals are here, incorporate in the test blocks
                #              [0]     cval   ->[cval] test par   stat ->{statistic}
                printf("    ID: %s col: %d value: %s is %s the %s (of bin_%s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,
                    $statistic,
                    $binNum)
                    if ( $self->testSub($fieldsAR->[$cval],$test,$max) );
                    #
                printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of bin_%s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,
                    $statistic,
                    $binNum) 
                    if ( $self->testSub($fieldsAR->[$cval],$test,$max) );  
            }    
        }
        ####### bin_N stats ##############################
        if ($statistic eq 'mean')
        {
            # parse histo to find actual bin_N for this value as $binNum
            my ($binNum, $lowerBinVal, $upperBinVal, $proportion) = $self->parseHisto($cval,$fieldsAR->[$cval]);
            if ($binNum ==$parameter)
            {
                printf("   --> val %s is in bin: %d == testBin: %d $nl",$fieldsAR->[$cval],$binNum,$parameter );
                #   parse the histobin and test the fVal
                my ($count,$min,$max,$mean) = $self->parseHistobin($cval,$binNum);
                #   now that these histobin vals are here, incorporate in the test blocks
                #              [0]     cval   ->[cval] test par   stat ->{statistic}
                printf("    ID: %s col: %d value: %s is %s the %s (of bin_%s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,
                    $statistic,
                    $binNum)
                    if ( $self->testSub($fieldsAR->[$cval],$test,$mean) );
                    #
                printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of bin_%s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,
                    $statistic,
                    $binNum) 
                    if ( $self->testSub($fieldsAR->[$cval],$test,$mean) );  
            }    
        }
        #######        
        ####### D distribution stats ##############################
        if ($statistic eq 'dmin' )
        {
            #              [0]     cval   ->[cval] test par   stat ->{statistic}
            printf("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,

                $statistic,
                $self->{_statsCol}->[$cval]->{$statistic}) 
                if ( $self->testSub($fieldsAR->[$cval],$test,$self->{_statsCol}->[$cval]->{$statistic} ));
                #
            printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,

                $statistic,
                $self->{_statsCol}->[$cval]->{$statistic}) 
                if ( $self->testSub($fieldsAR->[$cval],$test,$self->{_statsCol}->[$cval]->{$statistic} ));
            
        }
        ####### D distribution stats ##############################
        if ( $statistic eq 'dmax' )
        {
            #              [0]     cval   ->[cval] test par   stat ->{statistic}
            printf("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,

                $statistic,
                $self->{_statsCol}->[$cval]->{$statistic}) 
                if ( $self->testSub($fieldsAR->[$cval],$test,$self->{_statsCol}->[$cval]->{$statistic} ));
                #
            printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,

                $statistic,
                $self->{_statsCol}->[$cval]->{$statistic}) 
                if ( $self->testSub($fieldsAR->[$cval],$test,$self->{_statsCol}->[$cval]->{$statistic} ));            
        }
        ####### D distribution stats ##############################
        if ($statistic eq 'dmean')
        {
                #              [0]     cval   ->[cval] test par   stat ->{statistic}
                printf("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,

                    $statistic,
                    $self->{_statsCol}->[$cval]->{$statistic}) 
                    if ( $self->testSub($fieldsAR->[$cval],$test,$self->{_statsCol}->[$cval]->{$statistic} ));
                    #
                printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $test,

                    $statistic,
                    $self->{_statsCol}->[$cval]->{$statistic}) 
                    if ( $self->testSub($fieldsAR->[$cval],$test,$self->{_statsCol}->[$cval]->{$statistic} ));
           
        }
        #######
        if ($statistic eq 'year2030')
        {
            my $january2030 = 1893474001;
        
            printf("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,
                $statistic,
                $parameter) 
                if ( $self->testSub( $fieldsAR->[$cval],$test,$january2030 ) );
                #
            printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,
                $statistic,
                $parameter) 
                if ( $self->testSub( $fieldsAR->[$cval],$test,$january2030 ) );
        }
        #######
        if ($statistic eq 'epochtime')
        {
            my $january2030 = 1893474001;
        
            printf("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,
                $statistic,
                $parameter) 
                if ( $self->testSub( $fieldsAR->[$cval],$test,$parameter ) );
                #
            printf $auditOutHandle ("    ID: %s col: %d value: %s is %s the %s (of %s) $nl",
                $fieldsAR->[ $self->{_recordIDcolumn} ],
                $cval,
                $fieldsAR->[$cval],
                $test,
                $statistic,
                $parameter) 
                if ( $self->testSub( $fieldsAR->[$cval],$test,$parameter ) );
        }
        #######
        if ($statistic eq 'histo')
        {
            my $january2030 = 1893474001;
            #   parse histogram
            printf( "    HISTO: [%s] $nl",$self->{_statsCol}->[$cval]->{'histo'});
            #
            # parse histo
            my ($binNum, $lowerBinVal, $upperBinVal, $proportion) = $self->parseHisto($cval,$fieldsAR->[$cval]);
            #
            printf("    col: %d val: %s is in bin %d between (%s and %s) with %01.3f of the values. $nl",
                $cval,
                $fieldsAR->[$cval], 
                $binNum, $lowerBinVal, 
                $upperBinVal, 
                $proportion); 
                #
            printf $auditOutHandle ("    col: %d val: %s is in bin %d between (%s and %s) with %01.3f of the values. $nl",
                $cval,
                $fieldsAR->[$cval], 
                $binNum, $lowerBinVal, 
                $upperBinVal, 
                $proportion);       
            
            if ($test eq 'NotInBin')
            {
                printf("    ID: %s col: %s value: %s is in the %d bin (of %s) NotIn bin %s $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $binNum,
                    $self->{_statsCol}->[$cval]->{'histo'},
                    $parameter, 
                    )  if ($binNum != $parameter );
                    #
                printf $auditOutHandle ("    ID: %s col: %s value: %s is in the %d bin (of %s) NotIn bin %s $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $binNum,
                    $self->{_statsCol}->[$cval]->{'histo'},
                    $parameter, 
                    )  if ($binNum != $parameter );
            }
            #
            if ($test eq 'IsInBin')
            {
                printf("    ID: %s col: %s value: %s is in the %d bin (of %s) IsIn bin %s $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $binNum,
                    $self->{_statsCol}->[$cval]->{'histo'},
                    $parameter, 
                    )  if ($binNum == $parameter );
                    #
                printf $auditOutHandle ("    ID: %s col: %s value: %s is in the %d bin (of %s) IsIn bin %s $nl",
                    $fieldsAR->[ $self->{_recordIDcolumn} ],
                    $cval,
                    $fieldsAR->[$cval],
                    $binNum,
                    $self->{_statsCol}->[$cval]->{'histo'},
                    $parameter, 
                    )  if ($binNum == $parameter );
            }
        }    
    }
}
####
sub parseHisto
{
    my $self=shift;
    my $cval=shift;             # column num
    my $currentValue=shift;     # value at $fieldsAR->[$cval]
    #
    my $histoString = $self->{_statsCol}->[$cval]->{'histo'};
    my ($bin0count,$firstBin,$bin1count,$secondBin,$bin2count,$thirdBin,$bin3count) =
            split('<',$histoString);
    ##printf("    %d | %s | %d | %s | %d | %s | %d $nl",
    ##        $bin0count,$firstBin,$bin1count,$secondBin,$bin2count,$thirdBin,$bin3count);
    #   total population
    my $totalPop =  $bin0count+$bin1count+$bin2count+$bin3count + 1; # to avoid /0
    my $theProportion;
           
    #   describe where this value is in the distribution
    if ($currentValue <= $firstBin)
    {
        $theProportion = $bin0count / $totalPop;
        ##printf( "    CurVal: %s is in bin 0 and < %s Proportion: %03f $nl",
        ##    $currentValue,$firstBin,$theProportion);
        return (0, 0, $firstBin, $theProportion);    
    }   
    if ($currentValue <= $secondBin)
    {
        $theProportion = $bin1count / $totalPop;
        ##printf( "    CurVal: %s is in bin 1 and < %s Proportion: %03f $nl",
        ##    $currentValue,$secondBin,$theProportion);
        return (1, $firstBin, $secondBin, $theProportion);    
    } 
    if ($currentValue <= $thirdBin)
    {
        $theProportion = $bin2count / $totalPop;
        ##printf( "    CurVal: %s is in bin 2 and < %s Proportion: %03f $nl",
        ##    $currentValue,$thirdBin,$theProportion);
        return (2, $secondBin, $thirdBin, $theProportion);           
    } 
    if ($currentValue > $thirdBin)
    {
        $theProportion = $bin3count / $totalPop;
        ##printf( "    CurVal: %s is in bin 3 and < %s Proportion: %03f $nl",
        ##    $currentValue,$thirdBin,$theProportion);
        return (3, $thirdBin, 'INF', $theProportion);    

    }          
    #                
    ##printf("    InParseHisto. col: %d val: %s $nl",$cval,$currentValue);
    
}
####
#   is this still used?
####
sub showFields
{
    my $self=shift;
    my $fieldsAR=shift;
    my $colsToShowAR=shift;
    print "Showing fields: " if ($log);
    print Dumper($colsToShowAR) if ($log);
    print "---- $nl" if ($log);
    #
    #   extract opCode and opVal from the AR
    #
    foreach my $colOp ( @{$colsToShowAR} )        # this is a 3-tuple:  "col:opCode:opVal"
    {
        my ($cval,$opCode,$opVal) = split(":",$colOp);
        print "Operations to perform: $cval,$opCode,$opVal $nl";
        my $fixedVal;
        $fixedVal = $fieldsAR->[$cval] / $opVal if ($opCode eq 'div');      # fixed error?
        $fixedVal = $fieldsAR->[$cval] * $opVal if ($opCode eq 'mul');      # fixed error?
        $fixedVal = $fieldsAR->[$cval] + $opVal if ($opCode eq 'add');      # fixed error?
        $fixedVal = $fieldsAR->[$cval] - $opVal if ($opCode eq 'sub');      # fixed error?
        printf("col: %d  oldName: [%s] val: [%s] fixedVal: [%s] $nl",
            $cval,
            $self->{_oldFieldNamesAR}->[$cval],
            $fieldsAR->[$cval],
            $fixedVal,
            );
    }
}
#
############################################################
#   this entry point is for the mapping and json conversion
sub readCSV
{
    my $self=shift;
    my $csvFile=shift;
    my $colNamesAR=shift;
    my $jsonFileOutput=shift;
    my $maxRows=shift;
    #
    die "Could not open json file $jsonFileOutput " if (!open  my $jfhandle, ">", $jsonFileOutput);
    
    print STDERR "Now file: $csvFile $nl";
    $count=0;       # reset the counter
    open(my $data, '<', $csvFile) or die "Could not open $csvFile";
    print "Now file: $csvFile $nl";
    
    while (my $line = <$data>)
    {
        if ($csv->parse($line))
        {
            $count += 1;
            print STDERR "$csvFile $count\n" if ($count % 10000 == 0);
            
	        my @fields = $csv->fields();
	        print "Parsing record $count $nl" if ($log);
	        $self->parseAndDispatchLine(\@fields,$colNamesAR,$jfhandle);
	        #
	        if ($count >= $maxRows)
	        {
	            print "-------------------- done -------------- $nl";
	            #print Dumper($self);
	            print "MaxRows $maxRows exceeded. Set higher using -m <maxNumberOfRows> $nl";
	            # die "Reached max $count";
	            return;
	        }	
        }
    }
}    
####
#   
####
sub parseAndDispatchLine
{
    my $self=shift;
    my $fieldsAR=shift;
    my $colNamesAR=shift;
    my $jfhandle=shift;
    
    #print "Read a csv record. Parsing $nl";
    
    my $fno=0;
    foreach my $field (@{$fieldsAR})
    {
        ##printf("Field: %d [%s] = [%s] $nl",$fno,$colNamesAR->[$fno],$field);
        $OBJECT->{$colNamesAR->[$fno]} = $field;
        $convOBJECT->{$self->{_convOBJECT}->[$fno]} = $field;
        $fno += 1;
    }
    #   object created for record
    #print "Writing object to JSON $nl";
    #print Dumper($OBJECT);
    #print "convOBJECT ";
    #print Dumper($convOBJECT);
    #
    #   write the original object fields to json
    ##$self->writeJSONtoFile($OBJECT,$jfhandle);
    #   then write the convOBJECT to json if needed
    $self->writeJSONtoFile($convOBJECT,$jfhandle);
    #   now export object as json
}
####
# sFieldName,sDataType,sMinVal,sMaxVal,cFieldName,cDataType,cMinVal,cMaxVal
#
#   NOT USED
####
sub readHeader
{
    my $self=shift;
    my $lin=shift;
    print " Read schema conversion header line is: $lin $nl";
    $lin =~ s/\%//;
    my @headerFields = split(',',$lin);
    $self->{_convHeaderFieldsAR} = \@headerFields;
    #print Dumper($self);
    
}
####
#   Call readConversionSchema before doing stats_ranges or audit
#   Reads the schemaMappingCSV file, and populates oldFieldNames, and new*
####
sub readConversionSchema
{
    my $self=shift;
    my $file=shift;
    $self->{_schemaMappingFileCSV} = $file;
    print "SchemaMappingFileCSV is $file $nl";
    
    if (open my $SCHEMA, "<", $file)
    {
        my @convOBJECT;                 # arrayref of new colnames for JSON
        my @oldFieldNames;
        my @oldDataTypes;
        my @oldMinVal;
        my @oldMaxVal;
        my @newFieldNames;
        my @newDataTypes;
        my @newMinVal;
        my @newMaxVal;
        
        while (my $lin=<$SCHEMA>)
        {
            chomp($lin);
            $lin =~ s/\n//g;        # chomp doesnt seem to do it
            
            next if ($lin =~ /#/);
            if ($lin =~ /%/)
            {   
                $self->readHeader($lin);
                next;
            } 
            my @schemaFields = split(',',$lin);
            $self->{_convFieldMapAR}->{_oldField}->{$schemaFields[0]} = $schemaFields[4];
            #   read dataType field and store it for later auditing
            push @oldFieldNames,$schemaFields[0];
            push @oldDataTypes,$schemaFields[1];
            push @oldMinVal,$schemaFields[2];
            push @oldMaxVal,$schemaFields[3];
            
            push @newFieldNames,$schemaFields[4];
            push @newDataTypes,$schemaFields[5];
            push @newMinVal,$schemaFields[6];
            push @newMaxVal,$schemaFields[7];
            print $lin,$nl;
            push @convOBJECT,$schemaFields[4];     # add the colname
        }
        #
        $self->{_oldFieldNamesAR}   = \@oldFieldNames;
        $self->{_oldDataTypesAR}    = \@oldDataTypes;
        $self->{_oldMinValAR}       = \@oldMinVal;
        $self->{_oldMaxValAR}       = \@oldMaxVal;
        $self->{_newFieldNamesAR}   = \@newFieldNames;
        $self->{_newDataTypesAR}    = \@newDataTypes;
        $self->{_newMinValAR}       = \@newMinVal;
        $self->{_newMaxValAR}       = \@newMaxVal;
        
        #   now, connect the convOBJECT
        $self->{_convOBJECT} = \@convOBJECT;
        print "Conv mapping ",Dumper($self) if ($log);
    } else {
        die "Couldnt open schema file: $file ";
    }
}
###
sub writeOutColumnNamesTypes
{
    my $self=shift;
    my $numCols;
    foreach my $element (@{$self->{_oldFieldNamesAR}})
    {
        $numCols += 1;
    }
    print "Found $numCols columns $nl";
    
    foreach my $colNum (0..($numCols-1))
    {
        print "ColNum: $colNum ColName: $self->{_oldFieldNamesAR}->[$colNum] $nl";
    }
     
}
####
sub read_stats_report
{
    my $self=shift;
    my $reportFileCSV=shift;
    print "=== Reading $reportFileCSV === $nl";
    #
    if (open my $OR,"<",$reportFileCSV)
    {
    #   read it
        my $currentCol;         # undef
        while (my $rec=<$OR>)
        {
            chomp($rec);
            #printf("%s $nl",$rec);
            my @ofields = split('\|',$rec);      # | separates k:[v]

            foreach my $kvp ( @ofields )
            {
                #print "KVP: [$kvp] $nl";
                my ($key,$val) = split("\:",$kvp);
                # clean the $key and $val
                $key =~ s/\s+//g;
                $val =~ s/\s+//g;
                $val =~ s/\[//g;
                $val =~ s/\]//g;
                #
                $currentCol = $val if ($key eq 'col');  # set it first in the loop
                #
                $self->{_statsCol}->[$currentCol]->{$key} = $val;
            }
        }
    } else {
        die "Couldnt open stats report file: $reportFileCSV";
    }
}
#############################################


1;



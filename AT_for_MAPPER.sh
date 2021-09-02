#   AT_for_MAPPER.sh
#   shell script approach to test audit/DSL
#
echo    'Run this script in the MAPPER/ directory'
#
echo    'Set the executable to perl CSV_stats_ranges.pl'
echo    '    or csv_stats_ranges.out '

############### first do stats ranges to generate stats_ranges_report ##########
##csr='perl CSV_stats_ranges.pl '
csr='./UbuntuAouts/csv_stats_ranges.out '
##echo $csr
##$csr -l tests/UnitTestingData.csv -k 1 -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt

############### test csv to json mapping #######################################
#c2j='perl CSVtoJSON.pl '
c2j='./UbuntuAouts/csvtojson.out '
echo $c2j
$c2j -l tests/UnitTestingData.csv -s tests/Unicauca2019_schemaMap.csv -j tests/output.json

##aud='perl CSV_auditor.pl'
aud='./UbuntuAouts/csv_auditor.out '
##echo $aud

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '13:>:epochtime:1555966971' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit13_gt_epochtime_1555966971.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '7:NotIn:histo:2' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_7_notin_histo_2.txt 

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '7:IsIn:histo:2' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_7_IsIn_histo_2.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '3:>:dmin:D' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_3_gt_dmin_D.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '3:<:dmax:D' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_3_lt_dmax_D.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '3:=:dmean:D' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_3_eq_dmean_D.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '8:NotIn:histo:2' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_8_NotIn_histo_2.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:<:min:0' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_lt_min_0.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:>:min:1' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_gt_min_1.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:<:min:2' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_lt_min_2.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:=:min:3' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_eq_min_3.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:<:max:0' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_lt_max_0.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:>:max:1' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_gt_max_1.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:<:max:2' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_lt_max_2.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:=:max:3' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_eq_max_3.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:<:mean:0' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_lt_mean_0.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:>:mean:1' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_gt_mean_1.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:<:mean:2' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_lt_mean_2.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '1:=:mean:3' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_1_eq_mean_3.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '3:<:value:49175' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_3_lt_value_49175.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '3:>:value:49175' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_3_gt_value_49175.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '3:=:value:49175' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_3_eq_value_49175.txt

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '13:>:epochtime:1555966971' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_13_gt_epochtime_1555966971.txt 

$aud -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c '13:<:year2030:2030' -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/Audit_13_lt_year2030.txt



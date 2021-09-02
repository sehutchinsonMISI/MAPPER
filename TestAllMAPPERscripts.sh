#   testing.sh
#
echo "CSV_stats_ranges.pl"
perl CSV_stats_ranges.pl -l tests/UnitTestingData.csv -k 1 -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt

echo "CSV_auditor.pl"
perl CSV_auditor.pl -l tests/UnitTestingData.csv -k 1 -m 10000 -i 0 -c "13:>:epochtime:1555966971" -s tests/Unicauca2019_schemaMap.csv -r tests/stats_ranges_report.txt -o tests/AuditReportOutput.txt

echo "CSVtoJSON.pl"
perl CSVtoJSON.pl -l tests/UnitTestingData.csv -s tests/Unicauca2019_schemaMap.csv -j tests/output.json

echo "Test the DSL clauses in auditor."
echo "Be sure to have run CSV_stats_ranges.pl to generate the tests/stats_ranges_report.txt file."
perl AT_for_MAPPER.pl -m 100 -i 0

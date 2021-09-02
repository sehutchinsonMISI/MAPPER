#   generate_pp_packed_executables.sh
#
#   generates ./UbuntuAouts/csv_stats_ranges.out
#             ./UbuntuAouts/csv_auditor.out
#             ./UbuntuAouts/csvtojson.out
#
#   uses pp (libpar-packer-perl)
#
#   run in the MAPPER directory with *.pl files
#
pp -o UbuntuAouts/csv_stats_ranges.out CSV_stats_ranges.pl

pp -o UbuntuAouts/csv_auditor.out CSV_auditor.pl

pp -o UbuntuAouts/csvtojson.out CSVtoJSON.pl


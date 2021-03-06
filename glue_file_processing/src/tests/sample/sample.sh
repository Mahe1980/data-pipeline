head -n 1 Holdings20191206121314.csv > Holdings20191206121315.csv

tail -n +2 Holdings20191206121314.csv \
  | sed -E 's/"-?[0-9]+\.[0-9]+"/"-3.14"/g' \
  | sed -E 's/"-?[0-9]+"/"-5"/g' \
  | sed -E 's/"-?[0-9]+\.[0-9]+%"/"-3.14%"/g' \
  | sed 's/"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]+[0-9][0-9]:[0-9][0-9]"/"2019-01-01T00:00:00+00:00"/g' \
  | sed 's/"[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]"/"2019-01-01"/g' \
  | sed 's/"[[:alpha:][:space:]()]\+"/"something"/g' \
  | sed 's/"[[:alpha:][:space:]()\.&+-]\+[+[:digit:][:alpha:][:space:]()-]*"/"something"/g' \
  | sed 's/"[[:alpha:][:space:]()\.&+-]*[+[:digit:][:alpha:][:space:]()-]*:[[:alpha:][:space:]()\.&+-]\+[+[:digit:][:alpha:][:space:]()-]*"/"something"/g' \
  >> Holdings20191206121315.csv

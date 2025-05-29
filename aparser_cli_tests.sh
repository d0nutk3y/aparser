#!/bin/bash
clear

negative_test() {
  echo "[assert FAIL]------------------------ $*"
  python "$@"
  echo ''
};

positive_test() {
  echo "[assert SUCCESS]--------------------- $*"
  python "$@"
  echo ''
};

# Common parsing tests
#positive_test aparser_cli.py -h
#negative_test aparser_cli.py -t fff
#negative_test aparser_cli.py -t fff -of avi
#negative_test aparser_cli.py -t fff -o ooo -of txt
#negative_test aparser_cli.py -t notexisting -o ooo -of csv
#negative_test aparser_cli.py -t notexisting -o ooo -of json

# Real files test
positive_test aparser_cli.py -t ./test_logs/web_stress -o ./test_results/web_stress.csv -of csv
positive_test aparser_cli.py -t ./test_logs/web_stress -o ./test_results/web_stress.json -of json
#positive_test aparser_cli.py -t ./test_logs/web_stress -o ./test_results/web_stress_same.csv

#!/bin/bash
 
usage()
{
cat <<EOF
 
Usage: $(basename $0) [OPTIONS]
 
Script to produce HTML tables from delimited input. Delimiter can be specified
as an optional argument. If omitted, script defaults to comma.
 
Options:
 
  -        Use STDIN as source of delimited data.
 
  -d       Specify delimiter to look for, instead of comma.
 
  -f       Specify file to use as source of delimited data.
 
  --head   Treat first line as header, enclosing in <thead> and <th> tags.
 
  --foot   Treat last line as footer, enclosing in <tfoot> and <th> tags. 
 
  --help   Print this help message.
 
Examples:
 
  1. $(basename $0) -f input.csv
 
  Above will parse file 'input.csv' with comma as the field separator and
  output HTML tables to STDOUT.
 
  2. $(basename $0) -d '|' - < input.psv > output.htm
 
  Above will parse file "input.psv", looking for the pipe character as the
  delimiter, then output results to "output.htm".
 
  3. $(basename $0) -d '\t' --head --foot -f input.tsv
 
  Above will parse file "input.tsv", looking for tab as the delimiter, then
  process first and last lines as header/footer (that contain data labels).
 
  4. sed 's/|/,/g' input.psv | $(basename $0) -
 
  In this example, file "input.psv" is pre-processed in sed, then output is
  sent to $(basename $0), with the "-" option, to use STDIN as source of
  delimited data.
 
EOF
}
 
while [ "$1" ]; do
  case "$1" in
    -d)
      shift
      d="$1"
      ;;
    -f)
      shift
      f="$1"
      ;;
    --foot)
      foot="-v ftr=1"
      ;;
    --help)
      usage
      exit 0
      ;;
    --head)
      head="-v hdr=1"
      ;;
    -f)
      shift
      f="$1"
      ;;
    -)
      data=$(cat -)
      ;;
    -*)
      echo "ERROR: unknown option '$1'"
      echo "see '--help' for usage"
      exit 1
      ;;
  esac
  shift
done
 
if [ -z "$d" ]; then
  d=","
fi
 
if [ -f "$f" ]; then
  data=$(sed '/^$/d' $f)
else
  data=$(sed '/^$/d' <<< "$data")
fi
 
last=$(wc -l <<< "$data")
 
awk -F "$d" -v last=$last $head $foot '
  BEGIN {
    print "  <table>"
  }       
  {
    gsub(/</, "\\&lt;")
    gsub(/>/, "\\&gt;")
    if(NR == 1 && hdr) {  
      printf "    <thead>\n"
    gsub(/&/, "\\&gt;")    }
    if(NR == last && ftr) {  
      printf "    <tfoot>\n"
    }
    print "      <tr>"
    for(f = 1; f <= NF; f++)  {
      if((NR == 1 && hdr) || (NR == last && ftr)) {
        printf "        <th>%s</th>\n", $f
      }
      else printf "        <td>%s</td>\n", $f
    }     
    print "      </tr>"
    if(NR == 1 && hdr) {
      printf "    </thead>\n"
    }
    if(NR == last && ftr) {
      printf "    </tfoot>\n"
    }
  }       
  END {
    print "  </table>"
  }
' <<< "$data"

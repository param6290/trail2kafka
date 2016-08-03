###
# Purpose : generate report for hdfs space usage on the cluster
# 
###


## Defining Constants
TEMP_DATA_DIR=./.tmp


## Clean Previous Generated Files
## Sanity Checks.


##
# Purpose : 
##
function initialize_removefiles(){

	if [[ -e $1 ]]
	then
		rm  "$TEMP_DATA_DIR"/"$1"
		
	fi
}



if [[ -e .tmp ]]
then
	:
else
	mkdir .tmp
fi

if [[ -e "$TEMP_DATA_DIR"/input.txt ]]
then
	rm $TEMP_DATA_DIR/input.txt
fi

if [[ -e "$TEMP_DATA_DIR"/result.html ]]
then
	rm "$TEMP_DATA_DIR"/result.html
fi




# main program

hdfs dfsadmin -report | head -n -2 | sed 's/:.*(/ : /' | sed 's/[\)]//g' > "$TEMP_DATA_DIR"/rough.txt
sed -i 1d "$TEMP_DATA_DIR"/rough.txt



sizeOfFile=$(wc -l  "$TEMP_DATA_DIR"/rough.txt | awk '{print $1}')
echo $sizeOfFile



sizeOfFile=$((sizeOfFile-4))
echo $sizeOfFile


for ((i=0;i<sizeOfFile;i++))
        do
        IFS=$'\n' read -d '' -r -a readInput < "$TEMP_DATA_DIR"/rough.txt
        done

for ((i=0;i<sizeOfFile;i++))
        do
        echo ${readInput[$i]} | sed -e 's/:/,/g' >> "$TEMP_DATA_DIR"/input.txt
        done


. ./csv2html.sh -f "$TEMP_DATA_DIR"/input.txt > "TEMP_DATA_DIR"/result.html


#IFS=$'\n' read -d '' -r -a InputArray < "$TEMP_DATA_DIR"/input.txt
#echo "STORAGE: SIZE:  ${InputArray[@]} "| awk -F : 'BEGIN{print "<table>"} {print "<tr>";for(j=1;j<NF;j++)print "<td>" $j "</td>";print "</tr>"} END{print "</table>"}'



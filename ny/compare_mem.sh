##
## Memory usage of each node
##
function mem()
{
i=0
a=()
while read line
do
		first_char=${line:0:1}
        if [[ $first_char == "#" ]]
        then
                echo "$line skipped"
                continue
        else
           a[$i]=$line
		   ((i++))		 
        fi
done < nodes
for host in ${a[@]}
do
	echo $line `ssh director@$host free -m  | grep "cache:" | awk '{ print " : " $4  }'`
done
}
mem

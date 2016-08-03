#
# purpose of the script : Copy Table data From Hadoop To DR Cluster 
# Amruta
#

## variables

CONF_FILE_PATH="./"
CONF_FILE_NAME="inputfile1"

HIVE_DATABASE_NAME="bsedb_impala"

WORKING_TABLE=""
TABLE_ARRAY=()

PR_LOCAL_PATH="/data2/copy-dims/"

# DR SPECIFIC Variables
DR_HOSTNEM="dr"
DR_LOCAL_PATH="/backup/copy-dims/copieddata"

LOAD_TABLE_DATA="/incoming/"
DR_DATABASE_NAME="bsedb_impala"


echo > /tmp/report.txt


##
## All Functions Defined Here...
##

#  Arguments :
#  Purpuse : read the talbe name file and populate the array.

function read_conf_file()
{
        while read line
        do
                TABLE_ARRAY[$i]=$line
                ((i++))
        done < "$CONF_FILE_PATH"/"$CONF_FILE_NAME"
        #echo ${TABLE_ARRAY[@]}
}

function copy_table_to_accessnode()
{
	echo "Querying hive metadata for the hdfs path of table : $1"
  	hdfs_path=`impala-shell -i 192.168.156.6 --quiet -q "use $HIVE_DATABASE_NAME; describe formatted $1" | grep Location | tail -1 | awk '{print $4}'`
        # check if $hdfs_path is not null...
        if [[ -n $hdfs_path ]]
        then
		if [[ -d "$PR_LOCAL_PATH"/"$1" ]]
		then
			echo "Table Exists!! Deleting the Table."
			rm -r "$PR_LOCAL_PATH"/"$1"
		fi
		hadoop fs -copyToLocal $hdfs_path "$PR_LOCAL_PATH"/"$1"
                echo "Table copied.."

        else
                echo " No Path ..... "
        fi

	if [[ -d "$PR_LOCAL_PATH"/"$1"/.impala_insert_staging ]]
	then
		rm -rf "$PR_LOCAL_PATH"/"$1"/.impala_insert_staging			
	fi

}

#
#
#
function copy_to_dr_accessnode(){
        scp -r "$PR_LOCAL_PATH/$1" dr:"$DR_LOCAL_PATH/"
        if [[ $? -eq 0 ]]
        then
                echo " Table Copied to Remote Location Successfully...... "
                echo "deleting the hidden folder impala_insert_staging from dr accessnode"
                ssh dr rm -rf "$DR_LOCAL_PATH"/"$1"/.impala_insert_staging
                echo "copying $1 file to DR Cluster"
		echo "Testing if the folder already exists in hdfs"
		echo "$LOAD_TABLE_DATA/$1"
		ssh dr hadoop fs -test -d "$LOAD_TABLE_DATA"/"$1"

		if [[ $? -eq 0 ]]
		then
			echo "Folder Exists!! Deleting the Folder."
			ssh dr hadoop fs -rmr "$LOAD_TABLE_DATA"/"$1"
		else
			echo "old folder didn't exist...."
		fi				

		echo "copying the fresh data..."
                ssh dr hadoop fs -copyFromLocal "$DR_LOCAL_PATH"/"$1" /incoming/
        else
                echo " Table Not Copied to Remote Location Successfully.... "
        fi
}
function LOAD_DR_HIVE_TABLE()
{

	echo "Changing the Permissions of the file...on hdfs"
	ssh dr hadoop fs -chmod 775 "$LOAD_TABLE_DATA/$1"	
	echo "Loading the data into table..."
	echo "LOAD DATA INPATH '""$LOAD_TABLE_DATA/$1""' OVERWRITE into table bsedb_impala.$1" | ssh dr "cat > /tmp/tmp_query.sql"
	ssh dr ./ravi.sh
	#impala-shell -q "load data inpath $LOAD_TABLE_DATA/$1 overwrite into table $DR_DATABASE_NAME.$1";
	if [[ $? -eq 0 ]]
	then
		echo "Table Loaded Successfully...."
	else
		echo "Table Loading Failed..."
	fi
	
}

# Argumetns
# $1 : Status
# $2 : success output
# $3 : failure output

function status_f()
{
        if [[ $1 -eq 0 ]]
        then
                echo "$2"
                return 0
        else
                echo "$3"
                return 1
        fi
}


##
## Functions End Here....
##

read_conf_file

for TABLE in ${TABLE_ARRAY[@]}
do
        # Step 1 : copy the table to access node.
        echo "Current Working Table $TABLE"
        copy_table_to_accessnode $TABLE
        status_f $? "table found and copied to local disk" "table not found or local copy error"

        #step 2
        echo "copying to dr accessnode"
        copy_to_dr_accessnode "$TABLE"
		
	#step 3
	#ssh dr load data inpath '/incoming/error_dim' overwrite into table bsedb_impala.error_dim;
	#ssh dr impala-shell -q "load data inpath $LOAD_TABLE_DATA/$1 overwrite into table $DR_DATABASE_NAME.$1";
	LOAD_DR_HIVE_TABLE "$TABLE"
	
	#step 4
	echo "Hadoop Table Count"
	query_str="select count(*) from $HIVE_DATABASE_NAME.$TABLE"
	echo $query_str > /tmp/count_query.sql
	echo $query_str | ssh dr "cat > /tmp/count_query.sql"
	PR_HADOOP_Count=`impala-shell -i 192.168.156.6 -B --quiet -f /tmp/count_query.sql`
	DR_HADOOP_Count=`ssh dr impala-shell -i 192.168.52.198 -B --quiet -f /tmp/count_query.sql`
	echo "$TABLE	$PR_HADOOP_Count	$DR_HADOOP_Count" >> /tmp/report.txt
done

#if [[ $HIVE_QUERY=$DR_QUERY ]]
#then
#	echo "Table data copied from hive to dr cluster.."
#else
#	echo "Table data not copied from hive to dr cluster.."

#fi

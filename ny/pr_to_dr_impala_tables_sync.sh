
# purpose of the script : Copy Table data From Hadoop To DR Cluster 


## variables

CONF_FILE_PATH="./"
CONF_FILE_NAME="inputfile1"

HIVE_DATABASE_NAME="bsedb_impala"

WORKING_TABLE=""
TABLE_ARRAY=()

PR_LOCAL_PATH="/data2/copy-dims/"
PR_LOCAL_SCHEMA_PATH="/data2/copy-dims/hive-schema"

# DR ACCESS NODE : 141.9.1.36

# DR SPECIFIC Variables
DR_HOSTNEM="dr"
DR_LOCAL_PATH="/backup/copy-dims/copieddata"
DR_LOCAL_SCHEMA_PATH="/backup/copy-dims/hive-schema"
LOAD_TABLE_DATA="/incoming/"
DR_DATABASE_NAME="bsedb_impala"


echo > /tmp/report.txt
echo

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
    echo
  	hdfs_path=`impala-shell -i 192.168.156.6 --quiet -q "use $HIVE_DATABASE_NAME; describe formatted $1" | grep Location | tail -1 | awk '{print $4}'`
        # check if $hdfs_path is not null...
        if [[ -n $hdfs_path ]]
        then
		if [[ -d "$PR_LOCAL_PATH"/"$1" ]]
		then
			echo "Table Exists!! Deleting the Table."
            echo
			rm -r "$PR_LOCAL_PATH"/"$1"
		fi
		hadoop fs -copyToLocal $hdfs_path "$PR_LOCAL_PATH"/"$1"
                echo "Table copied.."
                echo

        else
                echo " No Path ..... "
                echo
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
         ssh dr rm -rf "$DR_LOCAL_PATH"/"$1"
	 scp -r "$PR_LOCAL_PATH/$1" dr:"$DR_LOCAL_PATH/"
        if [[ $? -eq 0 ]]
        then
                echo " Table Copied to Remote Location Successfully...... "
                echo
                echo "deleting the hidden folder impala_insert_staging from dr accessnode"
                echo
                ssh dr rm -rf "$DR_LOCAL_PATH"/"$1"/_impala_insert_staging
                echo "copying $1 file to DR Cluster"
                echo
		echo "Testing if the folder already exists in hdfs"
        echo
		echo "$LOAD_TABLE_DATA/$1"
        echo
		ssh dr hadoop fs -test -d "$LOAD_TABLE_DATA"/"$1"

		if [[ $? -eq 0 ]]
		then
			echo "Folder Exists!! Deleting the Folder."
            echo
			ssh dr hadoop fs -rmr "$LOAD_TABLE_DATA"/"$1"
		else
			echo "old folder didn't exist...."
            echo
		fi				

		echo "copying the fresh data..."
        echo
                ssh dr hadoop fs -copyFromLocal "$DR_LOCAL_PATH"/"$1" /incoming/
		ssh dr rm -rf "$DR_LOCAL_PATH"/"$1"
        else
                echo " Table Not Copied to Remote Location Successfully.... "
                echo
        fi
}

# CHECK HIVE TABLE STATUS
function CHECK_HIVE_TABLE()
{
	echo "CHECKING HIVE TABLE STATUS FOR $1 ...."
    echo
#	ssh dr FLAG=`impala-shell -i imp -B -q "use $HIVE_DATABASE_NAME; show tables ;" | grep "$1" | wc -l`
    FLAG=`ssh dr "impala-shell -i imp --quiet -B -q \"use $HIVE_DATABASE_NAME; show tables;\" | grep '$1' | wc -l "`

	if [ $FLAG -eq 1 ]
	then
		echo "TABLE EXIST IN DR..!"
        echo
		PR_COLUMNS_COUNT=`impala-shell -i 192.168.156.6 --quiet -B -q "use $HIVE_DATABASE_NAME; describe $1 ;" | wc -l `	
#		DR_COLUMNS_COUNT= `ssh dr impala-shell -i imp --quiet -B -q "use $HIVE_DATABASE_NAME; describe $1 ;" | wc -l `
    
        DR_COLUMNS_COUNT=`ssh dr "impala-shell -i imp --quiet -B -q \"use $HIVE_DATABASE_NAME ;describe $1 ;\" | wc -l "`
			if [ $PR_COLUMNS_COUNT -eq $DR_COLUMNS_COUNT ]
			then
			echo "TABLE COLUMNS ARE UPTO DATE"
            echo
			#exit 1
			else
			echo "TABLE NOT EXIST IN DR"
            echo
			CREATE_HIVE_TABLE "$1"
			fi
	else
	 	CREATE_HIVE_TABLE "$1"
	fi
}




# CREATING HIVE TABLE
function CREATE_HIVE_TABLE()
{
	
	echo "STARTING FOR $1 ..."
    echo
	echo "Removing Previous Local schema for $1 ..."
    echo
	rm -rf "$PR_LOCAL_SCHEMA_PATH"/"$1"
	echo "Generating Schema..."
    echo
#	impala-shell -i 192.168.156.6 --quiet -B -q "use $HIVE_DATABASE_NAME; show create table $1" -o "$PR_LOCAL_SCHEMA_PATH"/"$1"
    impala-shell -i 192.168.156.6 --quiet -B -q"use $HIVE_DATABASE_NAME; show create table $1 ;" > "$PR_LOCAL_SCHEMA_PATH"/"$1" 


	echo "Performing Trim..."
    echo
	sed -i 's/bsenhdnm01/bsedrhdnm01/g' "$PR_LOCAL_SCHEMA_PATH"/"$1"
	sed -i "\$a;" "$PR_LOCAL_SCHEMA_PATH"/"$1"
	sed -i 's/\"//g' "$PR_LOCAL_SCHEMA_PATH"/"$1"
	echo "copying PR to DR..."
    echo
	scp -r "$PR_LOCAL_SCHEMA_PATH/$1" dr:"$DR_LOCAL_SCHEMA_PATH/"
	echo "Executing DDL at Dr..."
    echo
	ssh dr "impala-shell -i imp -f \"$DR_LOCAL_SCHEMA_PATH/$1\" "
	echo "Removing schema in Dr local..."
    echo
	ssh dr rm -rf "$DR_LOCAL_SCHEMA_PATH/$1"

}

function LOAD_DR_HIVE_TABLE()
{

	echo "Changing the Permissions of the file...on hdfs"
    echo
	ssh dr hadoop fs -chmod 775 "$LOAD_TABLE_DATA/$1"	
	echo "Loading the data into table..."
    echo
	echo "LOAD DATA INPATH '""$LOAD_TABLE_DATA/$1""' OVERWRITE into table bsedb_impala.$1" | ssh dr "cat > /tmp/tmp_query.sql"
    echo
	ssh dr ./ravi.sh
	#impala-shell -q "load data inpath $LOAD_TABLE_DATA/$1 overwrite into table $DR_DATABASE_NAME.$1";
	if [[ $? -eq 0 ]]
	then
		echo "Table Loaded Successfully....!!!"
        echo
	else
		echo "Table Loading Failed..."
        echo
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
        echo "Current Working Table $TABLE ...."
        echo
        copy_table_to_accessnode $TABLE
  #      status_f $? "table found and copied to local disk" "table not found or local copy error"

        #step 2
        echo "copying to dr accessnode..."
        echo
        copy_to_dr_accessnode "$TABLE"
	
	echo "creating table in DR..."
    echo
#	CREATE_HIVE_TABLE "$TABLE"	
    CHECK_HIVE_TABLE "$TABLE"
    #step 3
	#ssh dr load data inpath '/incoming/error_dim' overwrite into table bsedb_impala.error_dim;
	#ssh dr impala-shell -q "load data inpath $LOAD_TABLE_DATA/$1 overwrite into table $DR_DATABASE_NAME.$1";
	LOAD_DR_HIVE_TABLE "$TABLE"
	
	#step 4
	echo "Hadoop Table Count"
    echo
	query_str="select count(*) from $HIVE_DATABASE_NAME.$TABLE"
	echo $query_str > /tmp/count_query.sql
    echo
	echo $query_str | ssh dr "cat > /tmp/count_query.sql"
    echo
	PR_HADOOP_Count=`impala-shell -i 192.168.156.6 -B --quiet -f /tmp/count_query.sql`
	DR_HADOOP_Count=`ssh dr impala-shell -i 192.168.52.198 -B --quiet -f /tmp/count_query.sql`
    echo "...................................................................................................."
	echo " TABLE NAME : $TABLE | PR HADOOP COUNT : $PR_HADOOP_Count | DR HADOOP COUNT : $DR_HADOOP_Count" #>> /tmp/report.txt
    echo "...................................................................................................."
    echo
#    cat /tmp/report.txt | mail -s "DR : $TABLE" gopireddy.guthikonda@bseindia.com
#	scp /tmp/report.txt bouser@192.168.156.54 /appl/sqoop
done

#if [[ $HIVE_QUERY=$DR_QUERY ]]
#then
#	echo "Table data copied from hive to dr cluster.."
#else
#	echo "Table data not copied from hive to dr cluster.."

#fi

##
# Production Access Node  
##
STATUS_VAR=""
YEAR="2015"
COPY_STATUS_PATH=/tmp/status/incremental
REMOTE_COPY_STATUS_PATH=/tmp/status/remote
DR_ORDER_LOCAL_PATH=/backup/work/bseorderdata/equity/
DR_ORDER_HDFS_PATH=/work/bseorderdata/equity/
PR_ORDER_LOCAL_PATH=/data2/work/bseorderdata/equity/
PR_ORDER_HDFS_PATH=/work/bseorderdata/equity/
#DR_TRADE_PATH=/backup/work/bsetradedata/equity_trade_fact/
DR_TRADE_LOCAL_PATH=/backup/work/bsetradedata/equity_trade_fact/
DR_TRADE_HDFS_PATH=/work/bsetradedata/equity_trade_fact/
PR_TRADE_LOCAL_PATH=/data2/work/bsetradedata/equity_trade_fact/
PR_TRADE_HDFS_PATH=/work/bsetradedata/equity_trade_fact/


#LOG_ROOT_PATH="/var/log/dr/"
#SCRIPT_ORDER_LOG_PATH="$LOG_ROOT_PATH/order-daily"
#SCRIPT_TRADE_LOG_PATH="$LOG_ROOT_PATH/trade-daily"

#ORDER_OUTPUT_LOG="$SCRIPT_ORDER_LOG_PATH/order"_`date +"%d%m%Y"`".out"
#ORDER_ERROR_LOG="$SCRIPT_ORDER_LOG_PATH/order-error"_`date +"%d%m%Y"`".log"
#TRADE_OUTPUT_LOG="$SCRIPT_TRADE_LOG_PATH/trade"_`date +"%d%m%Y"`".out"
#TRADE_ERROR_LOG="$SCRIPT_TRADE_LOG_PATH/trade-error"_`date +"%d%m%Y"`".log"


COPY_DATE=""
COPY_TYPE=""

COPY_TYPE=$1 # trade or order
COPY_DATE=$2 # trade/order copy date


function usage
{
	echo '/var/local/incremental.sh trade/order <trade/order-date>'
}

echo "Initiating Copy For : $COPY_TYPE"
echo "Initiating Copy For : $COPY_DATE"

WORKING_DATE=""


LOG_ROOT_PATH="/var/log/dr/"
SCRIPT_ORDER_LOG_PATH="$LOG_ROOT_PATH/order-daily"
SCRIPT_TRADE_LOG_PATH="$LOG_ROOT_PATH/trade-daily"

#ORDER_OUTPUT_LOG="$SCRIPT_ORDER_LOG_PATH/order"_"$WORKING_DATE"".out"
#ORDER_ERROR_LOG="$SCRIPT_ORDER_LOG_PATH/order-error"$WORKING_DATE".log"
#TRADE_OUTPUT_LOG="$SCRIPT_TRADE_LOG_PATH/trade"_"$WORKING_DATE"".out"
#TRADE_ERROR_LOG="$SCRIPT_TRADE_LOG_PATH/trade-error$WORKING_DATE"".log"



if [ "$COPY_DATE" == "" ]
then
	pr_dt=`date +%Y-%m-%d`
	WORKING_DATE=`date -d "$pr_dt -1 days" +%Y-%m-%d`
	Y=`echo $WORKING_DATE | cut -d"-" -f1`
	M=`echo $WORKING_DATE | cut -d"-" -f2`
	D=`echo $WORKING_DATE | cut -d"-" -f3`
	MM=`echo $M | awk '{print $0+0}'`
	DD=`echo $D | awk '{print $0+0}'`
else
	WORKING_DATE="$COPY_DATE"
	Y=`echo $WORKING_DATE | cut -d"-" -f1`
	M=`echo $WORKING_DATE | cut -d"-" -f2`
	D=`echo $WORKING_DATE | cut -d"-" -f3`
	MM=`echo $M | awk '{print $0+0}'`
	DD=`echo $D | awk '{print $0+0}'`
fi

echo "Working Date Is : $WORKING_DATE"

exec 5>&1
exec 6>&2


ORDER_OUTPUT_LOG="$SCRIPT_ORDER_LOG_PATH/order"_"$WORKING_DATE"".out"
ORDER_ERROR_LOG="$SCRIPT_ORDER_LOG_PATH/order-error"$WORKING_DATE".log"
TRADE_OUTPUT_LOG="$SCRIPT_TRADE_LOG_PATH/trade"_"$WORKING_DATE"".out"
TRADE_ERROR_LOG="$SCRIPT_TRADE_LOG_PATH/trade-error$WORKING_DATE"".log"


#exec 1> "$ORDER_OUTPUT_LOG"
#exec 2> "$ORDER_ERROR_LOG"

#Copying file from hdfs to local
if [ "$COPY_TYPE" == "order" ]
then
	exec 1> "$ORDER_OUTPUT_LOG"
	exec 2> "$ORDER_ERROR_LOG"

	echo "Copying the order to local system.... "
	/usr/bin/hadoop fs -copyToLocal "$PR_ORDER_HDFS_PATH"/orderdate="$WORKING_DATE" "$PR_ORDER_LOCAL_PATH"
	if [ $? -eq 0 ]
	then
		echo "Copying order to the local file system is successfull..."
		echo "Creating the status file on production Access Node..."
		touch "$COPY_STATUS_PATH"/"$COPY_TYPE"/"$WORKING_DATE"_"$COPY_TYPE".PRSuccess
		echo "Copying the file to remote accessnode...."
		/usr/bin/scp -r "$PR_ORDER_LOCAL_PATH"/orderdate="$WORKING_DATE" 141.9.1.36:"$DR_ORDER_LOCAL_PATH"
		if [ $? -eq 0 ]
		then
			echo "SCP to DR Access Node is successfull..."
			echo "Uploading the file on DR Cluster..."
			ssh -n bouser@141.9.1.36 hadoop fs -copyFromLocal "$DR_ORDER_LOCAL_PATH"/orderdate="$WORKING_DATE" "$DR_ORDER_HDFS_PATH/$YEAR/"
			if [ $? -eq 0 ]
			then
	
				##@ modify code to add partition.
							
	 
				echo "Creating the status file on the DR Access Node"
				ssh -n bouser@141.9.1.36 touch "$REMOTE_COPY_STATUS_PATH"/"$COPY_TYPE"/"$WORKING_DATE"_"$COPY_TYPE".DRSuccess
	
				echo "Deleting the DR Accessnode Local Copy of orderdate=$WORKING_DATE"
                                ssh -n bouser@141.9.1.36 rm -rf "$DR_ORDER_LOCAL_PATH"/orderdate="$WORKING_DATE"
                                echo "Deleting the PR Accessnode Local Copy of orderdate=$WORKING_DATE"
                                rm -rf "$PR_ORDER_LOCAL_PATH"/orderdate="$WORKING_DATE"
				ssh -n bouser@141.9.1.36 "impala-shell -i imp --quiet -q \"use bsedb_impala;alter table equity_order_fact add if not exists partition (orderdate='$WORKING_DATE',year=$Y,month=$M,day=$D) LOCATION '$DR_ORDER_HDFS_PATH/$YEAR/orderdate=$WORKING_DATE/year=$Y/month=$MM/day=$DD';\""

			else
				echo "order Upload to DR Cluster Failed..."
				ssh -n bouser@141.9.1.36 touch "$REMOTE_COPY_STATUS_PATH"/"$COPY_TYPE"/"$WORKING_DATE"_"$COPY_TYPE".DRFailed
			fi
		fi
	exec 1>&6 6>&-
	exec 2>&5 5>&-
	fi
	
elif [[ "$COPY_TYPE" == "trade" ]]
then
	exec 5>&1
	exec 6>&2

	exec 1> "$TRADE_OUTPUT_LOG"
        exec 2> "$TRADE_ERROR_LOG"	
	
	echo "Copying the trade to local system...."
	/usr/bin/hadoop fs -copyToLocal "$PR_TRADE_HDFS_PATH/"tradedate="$WORKING_DATE" "$PR_TRADE_LOCAL_PATH"
	if [[ $? -eq 0 ]]
	then
		echo "Copying the trade file to local system is successfull..."
		echo "Creating the status file on the production access node..."
		touch "$COPY_STATUS_PATH"/"$COPY_TYPE"/"$WORKING_DATE"_"$COPY_TYPE".PRSuccess
		echo "Copying the trade file to remote system..."
		/usr/bin/scp -r "$PR_TRADE_LOCAL_PATH/"tradedate="$WORKING_DATE" bouser@141.9.1.36:"$DR_TRADE_LOCAL_PATH"
		if [[ $? -eq 0 ]]
		then
			echo "SCP to DR access node is successfull..."
			echo "Uploading trade on DR Cluster"
			ssh -n bouser@141.9.1.36 hadoop fs -copyFromLocal "$DR_TRADE_LOCAL_PATH/"tradedate="$WORKING_DATE" "$DR_TRADE_HDFS_PATH"/"$YEAR"
			if [[ $? -eq 0 ]]
			then
				echo "Creating the status file for trade on the DR Access Node"
				ssh -n bouser@141.9.1.36 touch "$REMOTE_COPY_STATUS_PATH"/"$COPY_TYPE"/"$WORKING_DATE"_"$COPY_TYPE".DRSuccess
				
				# Cleaning the local files
				echo "Deleting the DR Accessnode Local Copy of tradedate=$WORKING_DATE"
				ssh -n bouser@141.9.1.36 rm -rf "$DR_TRADE_LOCAL_PATH"/tradedate="$WORKING_DATE"
				echo "Deleting the PR Accessnode Local Copy of tradedate=$WORKING_DATE"
				rm -rf "$PR_TRADE_LOCAL_PATH"/tradedate="$WORKING_DATE"
				
			else
				echo "trade Upload to DR Cluster Failed"
				ssh -n bouser@141.9.1.36 touch "$REMOTE_COPY_STATUS_PATH"/"$COPY_TYPE"/"$WORKING_DATE"_"$COPY_TYPE".DRFailed
			fi	
		fi
	exec 1>&6 6>&-
	exec 2>&5 5>&-

	fi
else 
	echo '/var/local/incremental trade/order <trade/order-date>'
fi




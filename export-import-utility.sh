#!/bin/bash
#
# Bash script to use Hbase ...
#
# Author:    Ivan Garcia <ivan.garcia@amk-technologies.com>
# Copyright: AMK Technologies, S.A. de C.V. 2019

#
# Method to Hbase export utility.
#
exportUtility() {
	HBASE_EXPORT='hbase org.apache.hadoop.hbase.mapreduce.Export'
	ARG_COMPRESS=''
	ARG_COMPRESS_CODEC=''
	ARG_MAP_SPECULATIVE=''
	ARG_REDUCE_SPECULATIVE=''
	ARG_CACHING=''
	TABLE_NAME=''
	HDFS_OUTPUT_PATH=''
	VERSION=0
	START_TIME=0
	END_TIME=0

	echo -n '-Dmapreduce.output.fileoutputformat.compress=(true/false): '
	read IS_COMPRESS
	if [ $IS_COMPRESS == 'true' ]
	then
		ARG_COMPRESS='-Dmapreduce.output.fileoutputformat.compress=true'
    	echo -e '-Dmapreduce.output.fileoutputformat.compress.codec=\n'
		echo -e '\ta) NONE\n \tb) SNAPPY\n \tc) LZO\n \td) LZ4\n \te) GZ\n'
		echo -n 'option: '
		read COMPRESSOR_OPTION
		case $COMPRESSOR_OPTION in
			a) ARG_COMPRESS_CODEC='-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.DefaultCodec';;
		    b) ARG_COMPRESS_CODEC='-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec';;
			c) ARG_COMPRESS_CODEC='-Dmapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzoCodec';;
			d) ARG_COMPRESS_CODEC='-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.Lz4Codec';;
			e) ARG_COMPRESS_CODEC='-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec';;
		  	*) 
				ARG_COMPRESS=''
				ARG_COMPRESS_CODEC='';;
		esac
	else
		ARG_COMPRESS=''
		ARG_COMPRESS_CODEC=''
	fi

	echo -n '-Dmapreduce.map.speculative=(true/false)(default true): '
	read IS_MAP_SPECULATIVE
	case $IS_MAP_SPECULATIVE in
		true) ARG_MAP_SPECULATIVE='-Dmapreduce.map.speculative=true';;
	    false) ARG_MAP_SPECULATIVE='-Dmapreduce.map.speculative=false';;
	  	*) 
			ARG_MAP_SPECULATIVE='';;
	esac
	
	echo -n '-Dmapreduce.reduce.speculative=(true/false)(default true): '
	read IS_REDUCE_SPECULATIVE
	case $IS_REDUCE_SPECULATIVE in
		true) ARG_REDUCE_SPECULATIVE='-Dmapreduce.reduce.speculative=true';;
	    false) ARG_REDUCE_SPECULATIVE='-Dmapreduce.reduce.speculative=false';;
	  	*) 
			ARG_REDUCE_SPECULATIVE='';;
	esac
	
	echo -n '-Dhbase.client.scanner.caching=(default 100): '
	read CACHING
	if [ $CACHING =~ $IS_NUMERIC ] ; then
		echo -n '*****************'
		CACHING=0
	fi
	if [ $CACHING -gt 100 ] ; then 
		ARG_CACHING="-Dhbase.client.scanner.caching=$CACHING"
	fi

	echo -n 'Enter table name: '
	read TABLE_NAME
	if [ $TABLE_NAME == '' ] ; then 
		echo -n 'TABLE_NAME is emty.'
		exit 1
	fi

	echo -n 'Enter HDFS output path: '
	read HDFS_OUTPUT_PATH
	if [ $HDFS_OUTPUT_PATH == '' ] ; then 
		echo -n 'HDFS_OUTPUT_PATH is emty.'
		exit 1
	fi

	echo -n 'Enter version of the rows (default 1): '
	read VERSION
	if [ $VERSION != [0-9] ] && [ $VERSION -lt 0 ] ; then
		VERSION=''
	fi		

	echo -n 'Start time (miliseconds): '
	read START_TIME
	if [ $START_TIME != [0-9] ] && [ $START_TIME -lt 0 ] ; then
		START_TIME=''
	fi			

	echo -n 'End time (miliseconds): '
	read END_TIME
	if [ $END_TIME != [0-9] ] && [ $END_TIME -lt 0 ] ; then
		END_TIME=''
	fi				

	COMMAND="$HBASE_EXPORT $ARG_COMPRESS $ARG_COMPRESS_CODEC $ARG_MAP_SPECULATIVE $ARG_REDUCE_SPECULATIVE $ARG_CACHING \"$TABLE_NAME\" \"$HDFS_OUTPUT_PATH\" $ROWS_VERSION $START_TIME $END_TIME"
	echo $COMMAND
	eval $COMMAND
}

#
# Method to Hbase import utility.
#
importUtility() {
	HBASE_IMPORT='hbase org.apache.hadoop.hbase.mapreduce.Import'
}


#
#
#
echo -n 'Enter the Hbase utility (Export/Import):  '
read HBASE_UTILITY

case $HBASE_UTILITY in
	Export) exportUtility;;
    Import) importUtility;;
  	*) echo -e "Hbase utility unknown. \n";;
esac
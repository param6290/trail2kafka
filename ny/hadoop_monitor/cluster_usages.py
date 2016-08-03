"""
__author__ = ravi.shekhar

Usage : Track the hdfs usage of Prod and DR Cluster

Send Email in Tabular Format as an Alert.
"""

import os
from datetime import datetime
from smail import send_alert

## Get the root of the project.
# Return Type : str(__absolutepath__)
##
def _get_project_root():
   return os.path.dirname(os.path.abspath(__file__))



## Global Variables
TEMP_FILE_LOC = _get_project_root() + "/tempfiles/"
PR_DFSUSAGE_FILE = "prod.dfsusage"
DR_DFSUSAGE_FILE = "dr.dfsusage"

current_date_time = str(datetime.today()).split('.')[0]

header_part = r'''
                    <!DOCTYPE html>
                    <html>
                        <head>
                            <style>
                                table, th, td {
                                    border: 1px solid black;
                                    border-collapse: collapse;
                                }
                                th, td {
                                    padding: 5px;
                                }
                            </style>
                        </head>
                        <body>
                '''
header_part += r'<strong>Date : ' + current_date_time + r'</strong>'
##
# This function will convert the elements of lists as an HTML table.
##
def _convert_to_html_table(table_list,cluster_name):
    if cluster_name is "prod":
        table_heading = r'<center><h3>Production Cluster Usage</h3></center>'
    if cluster_name is "dr":
        table_heading = r'<center><h3>DR Cluster Usage</h3></center>'
    
    table_string = r'<table style="width:100%">'
    for rows in table_list:
        table_string += r'<tr>'
        for item in rows:
            table_string += r'<th>' + item + r'</th>'
        table_string += r'</tr>'
        table_string += "\n"
    
    table_string += r'''
                        </table>
                        </body>
                        </html>
                    '''
    
    return table_heading + table_string




##
# Description : This function is dedicated to parse the dfsadmin report for any
# cluster.
# ARG : filename
# Returns a CSV format of usage
##
def _parse_dfsusage_file(filename):
    """ The output of this function has to be fed into html table convertor """
    filename = TEMP_FILE_LOC + filename # Locate the file
    file_handle = ''
    table_list = []
    table_header = []
    table_row1 = []
    try:
        file_handle = open(filename,'rt')
    except IOError as e:
        print "Exception Occurred While opening the file"
        print "{}".format(str(e))

    for line in file_handle.readlines():
        if line.startswith("DFS") or line.startswith("Present Capacity"):
            line_list = line.split(" ")
            column1 = line_list[0] + " " + line_list[1][:-1]
            column2 = line_list[2]
            if "%" not in column2:
                column2 = str(int(column2) // (1024*1024*1024*1024)) + "(TB)"
           
            table_header.append(column1.strip())
            table_row1.append(column2.strip())
    
    table_list.append(table_header)
    table_list.append(table_row1)

    return table_list
##
# Description : This function is dedicated to track the production space usage.
# 
##
def usage_prod():
	#CMD="hadoop dfsadmin -report > " + TEMP_FILE_LOC + PR_DFSUSAGE_FILE
    CMD="hadoop dfsadmin -report >" + TEMP_FILE_LOC + PR_DFSUSAGE_FILE
    try:
        os.system(CMD)
    except OSError as e:
        print "Exception Occurred : {}".format(str(e))
    return _parse_dfsusage_file("prod.dfsusage")


def usage_dr():
    CMD="ssh dr hadoop dfsadmin -report >" + TEMP_FILE_LOC + DR_DFSUSAGE_FILE
    try:
        os.system(CMD)
    except OSError as e:
        print "Exception Occurred : {}".format(str(e))
   
    return _parse_dfsusage_file("dr.dfsusage") 


if __name__ == "__main__":
    prod_list = usage_prod()
    html_prod = _convert_to_html_table(prod_list,"prod")
    dr_list = usage_dr()
    html_dr = _convert_to_html_table(dr_list,"dr")

    html_newline = r'<br/><br/>'
    combined_pr_dr_html = header_part + html_prod + html_newline + html_dr

    send_alert(combined_pr_dr_html) 
   #send_alert

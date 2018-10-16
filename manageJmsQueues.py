"""
The script contains a set of utilities for managing JMS queues on the Weblogic Server.
Available functionality:
    [1] List queues without listeners. The report will also contain count of current messages and current consumers.
    [2] List all queues with current messages. The report will  contain count of current messages and current consumers.
    [3] List DMQ queues with current messages. The report will also contain count of current messages.
    [4] Delete messages from a given queue.
    [5] Delete queues. Input: List of space separated queue names.
    [6] Move messages from one queue (e.g. DMQ) to another.
    [7] Get the queue information:  Queue name, Messages Current Count, Messages Received Count as well as some basic
        information about queues first and last messages (size, timestamp, etc)
Usage:
1. Execute: wlst manageJmsQueues.py -loadProperties manageJmsQueues_[ENV].properties
2. Select action
"""
import sys, os
import random
import os.path
import time
from time import gmtime, strftime, localtime
from weblogic.jms.extensions import JMSMessageInfo
from javax.jms import TextMessage
from javax.jms import ObjectMessage


def list_queues_without_listeners():
    """
    This function lists queues without listeners, i.e. having consumerCurrentCount = 0. 
    The report will also contain count of current messages and current consumers.
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) > 0:
            report = []
            for server in servers:
                log("INFO", "Searching queues on server " + server.name + "...")
                jms_runtime = server.getJMSRuntime()
                jms_servers = jms_runtime.getJMSServers()
                for jms_server in jms_servers:
                    destinations = jms_server.getDestinations()
                    for dest in destinations:
                        cons_cur_cnt = dest.consumersCurrentCount  # Current Consumer Count
                        name = dest.name  # Queue name
                        if cons_cur_cnt == 0 and "_dmq" not in name:  # Do not check DMQ queues
                            msg_cur_cnt = dest.messagesCurrentCount  # Current Messages Count
                            report.append([name, cons_cur_cnt, msg_cur_cnt])
            # Create report
            report_title = "REPORT: LIST QUEUES WITHOUT LISTENERS, " + url + ", " + cur_dt()
            col_names = ("QUEUE_NAME", "CUR_CONS#", "CUR_MSG#")
            create_report(report_title, report, col_names, is_sorted=True)
            f.write("\n")
        else:    
            log("WARNING", "No servers were found at " + url + ". Terminating the script...")
        log("INFO", "list_queues_without_listeners completed.")
    except (WLSTException, ValueError, NameError, Exception), e:
       log("ERROR", str(e))



def list_all_queues_with_current_messages():
    """
    This function lists all queues with current messages (i.e. queues with messagesCurrentCount > 0.
    The report will also contain count of current messages and current consumers.
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) > 0:
            report = []
            for server in servers:
                log("INFO", "Searching queues on server " + server.name + "...")
                jms_runtime = server.getJMSRuntime()
                jms_servers = jms_runtime.getJMSServers()
                for jms_server in jms_servers:
                    destinations = jms_server.getDestinations()
                    for dest in destinations:
                        name = dest.name
                        msg_cur_cnt = dest.messagesCurrentCount
                        if msg_cur_cnt > 0:
                            cons_cur_cnt = dest.consumersCurrentCount
                            report.append([name, cons_cur_cnt, msg_cur_cnt])
            # Create report
            report_title = "REPORT: LIST OF QUEUES WITH CURRENT MESSAGES, " + str(url) + ", " + cur_dt()
            col_names = ("QUEUE_NAME", "CUR_CONS#", "CUR_MSG#")
            create_report(report_title, report, col_names, is_sorted=True)
            f.write("\n")
        else:    
            log("WARNING", "No servers were found at " + url + ". Terminating the script...")
        log("INFO", "list_all_queues_with_current_messages completed.")
    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def list_dmq_queues_with_current_messages():
    """
    This function lists DMQ queues with current messages, 
    i.e. with name having "dmq" in the name and where messagesCurrentCount > 0. 
    The report will also contain count of current messages and current consumers.
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) > 0:
            report = []
            for server in servers:
                log("INFO", "Searching queues on server " + server.name + "...")
                jms_runtime = server.getJMSRuntime()
                jms_servers = jms_runtime.getJMSServers()
                for jms_server in jms_servers:
                    destinations = jms_server.getDestinations()
                    for dest in destinations:
                        name = dest.name
                        msg_cur_cnt = dest.messagesCurrentCount
                        if msg_cur_cnt > 0 and "_dmq" in name:
                            report.append([name, str(msg_cur_cnt)])
            # Create report
            report_title = "REPORT: LIST DMQ QUEUES WITH CURRENT MESSAGES, " + url + ", " + cur_dt()
            col_names = ("QUEUE_NAME", "CUR_MSG#")
            create_report(report_title, report, col_names, is_sorted=True)
            f.write("\n")
        else:    
            log("WARNING", "No servers were found at " + url + ". Terminating the script...")
        log("INFO", "list_dmq_queues_with_current_messages completed.")
    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def delete_messages_from_queue():
    """
    This function deletes all messages from a given queue.
    """
    while True:
        queue_name = raw_input("[INPUT] Enter queue name (e.g. WLMsgQueueName): ")
        if not queue_name:
            print(cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid name")
            continue
        else:
            break
    try:
        queue_name = queue_name.strip()
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) > 0:
            for server in servers:
                log("INFO", "Searching queues on server " + server.name + "...")
                jms_runtime = server.getJMSRuntime()
                jms_servers = jms_runtime.getJMSServers()
                for jms_server in jms_servers:
                    destinations = jms_server.getDestinations()
                    for dest in destinations:
                        name = get_queue_name(dest.name)
                        if queue_name == name:
                            msg_cur_cnt = dest.messagesCurrentCount
                            cons_cur_cnt = dest.consumersCurrentCount
                            print(cur_dt() + " [INFO] =====================")
                            log("INFO", "Name: " + str(name) + ", Current consumers: " + str(cons_cur_cnt)
                                + ", Current messages: " + str(msg_cur_cnt))
                            if msg_cur_cnt > 0:
                                del_msgs_choice = raw_input("[INPUT] Do you want to delete messages from this queue, Y/N [N]? ")
                                print("")
                                if del_msgs_choice.upper() == "Y":
                                    log("INFO", "Deleting " + str(msg_cur_cnt) + " messages...")
                                    dest.deleteMessages("")
                                    # Check current messages after delete
                                    msg_cur_cnt_new = dest.consumersCurrentCount
                                    if msg_cur_cnt_new == 0:
                                        log("INFO", "Successfully deleted " + str(msg_cur_cnt) + " messages")
                                    else:
                                        log("WARNING", "Deleted " + str(msg_cur_cnt - msg_cur_cnt_new) + " of "
                                            + str(msg_cur_cnt) + " messages. Repeat the procedure to delete the rest.")
                                else:
                                    log("INFO", "Skipping as per user prompt...")
                            else:
                                log("INFO", "The queue is empty. Skipping...")
        else:
            log("WARNING", "No servers were found at " + url + ". Terminating the script...")
        log("INFO", "delete_messages_from_queue completed.")
    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def delete_queues():
    """
    This function deletes JMS queues from the given list of queues.
    The list must be space separated and must contain at least one queue name, i.e. "WLMsgQueueName1"
    """
    report = []
    while True:
        queue_names = raw_input("[INPUT] Enter names of the queues separated by a space: ")
        if not queue_names:
            print(cur_dt() + "[ERROR] List of queue names cannot be empty and must contain at least one name")
            continue
        else:
            break
    print("")
    log("INFO", "Queues to delete: " + queue_names)
    queue_names = queue_names.split(" ")
    try:
        edit()
        startEdit()
        print("")
        my_jms_system_resources = cmo.JMSSystemResources
        changes_cnt = 0
        for queue_name in queue_names:
            queue_name = queue_name.strip()
            log("INFO", "Searching for queue '" + queue_name + "'...")
            cnt = 0
            for my_jms_system_resource in my_jms_system_resources:
                jms_module_name = my_jms_system_resource.name
                cd('/JMSSystemResources/' + jms_module_name + '/JMSResource/' + jms_module_name)
                queue_bean = cmo.lookupUniformDistributedQueue(queue_name)
                if queue_bean:
                    cnt += 1
                    print("")
                    log("INFO", "UniformDistributedQueue '" + queue_name + "' was found in JMS module '"
                        + jms_module_name + "'")
                    del_queue_choice = raw_input("[INPUT] Do you want to delete this queue, Y/N [N]? ")
                    if del_queue_choice.upper() == "Y":
                        cmo.destroyUniformDistributedQueue(queue_bean)
                        log("INFO", "UniformDistributedQueue '" + queue_name + "' deleted.")
                        report.append(["UniformDistributedQueue", queue_name, "Deleted"])
                    else:
                        report.append(["UniformDistributedQueue", queue_name, "Skipped by user"])
                else:
                    queue_bean = cmo.lookupQueue(queue_name)
                    if queue_bean:
                        cnt += 1
                        log("INFO", "Queue '" + queue_name + "' was found in JMS module '" + jms_module_name + "'")
                        del_queue_choice = raw_input("[INPUT] Do you want to delete this queue, Y/N [N]? ")
                        if del_queue_choice.upper() == "Y":
                            cmo.destroyQueue(queue_bean)
                            log("INFO", "Queue " + queue_name + " deleted.")
                            report.append(["Queue", queue_name, "Deleted"])
                        else:
                            report.append(["Queue", queue_name, "Skipped by user"])
                    else:
                        # Search in foreign servers
                        frn_srvs = cmo.getForeignServers()
                        for frn_srv in frn_srvs:
                            frn_dest_bean = frn_srv.lookupForeignDestination(queue_name)
                            if frn_dest_bean:
                                cnt += 1
                                cd('/JMSSystemResources/' + jms_module_name + '/JMSResource/' + jms_module_name
                                   + '/ForeignServers/' + frn_srv.name)
                                log("INFO", "ForeignDestination '" + queue_name + "' was found in JMS module '"
                                    + jms_module_name + "'")
                                del_queue_choice = raw_input("[INPUT] Do you want to delete this queue, Y/N [N]? ")
                                if del_queue_choice.upper() == "Y":
                                    cmo.destroyForeignDestination(frn_dest_bean)
                                    log("INFO", "ForeignDestination '" + queue_name + "' was deleted successfully.\n")
                                    report.append(["ForeignDestination", queue_name, "Deleted"])
                                else:
                                    report.append(["ForeignDestination", queue_name, "Skipped by user"])
                        continue
                    # TODO: Add topics
            if cnt == 0:
                log("INFO", "'" + queue_name + "' was not found in any JMS module.")
                report.append(["Queue", queue_name, "Not found"])
            changes_cnt += cnt
        print("")
        if changes_cnt > 0:
            log("INFO", "Saving changes...")
            save()
            log("INFO", "Activating session...")
            activate(block="true")
        else:
            log("INFO", "No changes were made. Canceling edit session...")
            cancelEdit('y')

        print("")
        report_title = "REPORT: DELETE QUEUES, " + url + ", " + cur_dt()
        col_names = ("OBJECT_TYPE", "OBJECT_NAME", "STATUS")
        create_report(report_title, report, col_names, is_sorted=True)
        f.write("\n")
        print("")
        log("INFO", "get_queue_info completed.")

    except ServiceUnavailableException:
        log("ERROR", str(e))
    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))
        log("INFO", "Undoing changes and canceling edit session...")
        undo("true", "y")
        cancelEdit("y")
        if report:
            report_title = "REPORT: DELETE QUEUES, " + url + ", " + cur_dt()
            col_names = ("OBJECT TYPE", "OBJECT NAME", "STATUS")
            create_report(report_title, report, col_names, is_sorted=True)
            f.write("\n")
        print("")


def move_messages():
    """
    This function moves messages from one queue (e.g. DMQ) to another. 
    Input: Names of the source queue (e.g. WLMsgRampe_Vare_PRMS_dmq) and the target queue (e.g. WLMsgRampe_Vare_PRMS).
    """
    while True:
        q_src_name = raw_input("[INPUT] Enter name of the source queue: ")
        if not q_src_name:
            print(cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid queue name.")
            continue
        else:
            break
    q_src_name = q_src_name.strip()
    print("")

    while True:
        q_trg_name = raw_input("[INPUT] Enter name of the target queue: ")
        if not q_trg_name:
            print(cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid queue name.")
            continue
        else:
            break
    q_trg_name = q_trg_name.strip()
    print("")

    log("INFO", "q_src_name: " + q_src_name + ", q_trg_name: " + q_trg_name)
    try:
        serverRuntime()
        print("")
        
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) > 0:
            report = []
            q_beans_list = []
            for server in servers:
                q_src_found = False
                q_trg_found = False
                log("INFO", "Processing server: " + server.name + "...") 
                jms_runtime = server.getJMSRuntime()
                jms_servers = jms_runtime.getJMSServers()
                for jms_server in jms_servers:
                    print(cur_dt() + "[INFO] Processing JMS Server: " + jms_server.name + "...")
                    destinations = jms_server.getDestinations()
                    for dest in destinations:
                        queue_name = get_queue_name(dest.name)  # TODO: Should be a better way.
                        if queue_name == q_src_name:
                            log("INFO", "Queue found: " + dest.name)
                            q_src_found = True
                            q_src_bean = dest
                            q_src_bean_msg_cur_cnt = q_src_bean.messagesCurrentCount
                        if queue_name == q_trg_name:
                            log("INFO", "Queue found: " + dest.name)
                            q_trg_found = True
                            q_trg_bean = dest
                        if q_src_found and q_trg_found:
                            break
                    if q_src_found and q_trg_found:
                        q_beans_list.append([q_src_bean, q_trg_bean, q_src_bean_msg_cur_cnt])
                        report.append([server.name, jms_server.name, q_src_name, q_trg_name, q_src_bean_msg_cur_cnt])
                        break  # go to next server
            report_title = "REPORT: QUEUES FOUND FOR MOVE MESSAGES, " + url + ", " + cur_dt()
            col_names = ("SERVER", "JMS SERVER", "SOURCE QUEUE", "TARGET QUEUE", "MESSAGES#")
            create_report(report_title, report, col_names, is_sorted=True)
            f.write("\n")

            mv_msgs_choice = raw_input("[INPUT] Do you want to move messages from '" + q_src_name + "' to '"
                                       + q_trg_name + "', Y/N [N]? ")
            if mv_msgs_choice.upper() == "Y":
                for row in q_beans_list:
                    q_src_bean_msg_cur_cnt = row[2]
                    if q_src_bean_msg_cur_cnt > 0:
                        q_src_bean = row[0]
                        q_trg_bean = row[1]
                        log("INFO", "Moving " + str(q_src_bean_msg_cur_cnt) + " messages from '"
                            + q_src_bean.name + "'...")
                        q_src_bean.moveMessages("", q_trg_bean.getDestinationInfo())
                        q_src_bean_msg_cur_cnt_new = q_src_bean.messagesCurrentCount
                        log("INFO", "Current messages on '" + q_src_bean.name + "': " + str(q_src_bean_msg_cur_cnt_new))
                        if q_src_bean_msg_cur_cnt_new == 0:
                            log("INFO", "Successfully moved " + str(q_src_bean_msg_cur_cnt) + " messages to '"
                                + q_trg_bean.name + "'.")
                        else:
                            log("WARNING", "There are still current messages on '" + q_trg_bean.name
                                + "'. Repeat the procedure.")
            else:
                log("WARNING", "Operation canceled by the user.")
        else:
            log("WARNING", "No servers were found at " + url + ". Terminating the script...")
        log("INFO", "move_messages completed.")
    except (ServiceUnavailableException, WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))
        if report:
            report_title = "REPORT: MOVE_MESSAGES , " + url + ", " + cur_dt()
            col_names = ("OBJECT TYPE", "OBJECT NAME", "STATUS")
            create_report(report_title, report, col_names, is_sorted=True)
            f.write("\n")
        print("")


def get_queue_info():
    """
    This function returns information on a given queue.
    """
    while True:
        queue_name = raw_input("[INPUT] Enter a queue name (e.g. WLMsgQueueName): ")
        if not queue_name:
            print(cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid name")
            continue
        else:
            break
    try:
        queue_name = queue_name.strip()
        log("INFO", "Entered queue name: " + queue_name)
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) > 0:
            col_names = ("PROPERTY", "VALUE")
            for server in servers:
                report = []
                report_title = "REPORT: INFORMATION ON QUEUE " + queue_name + ", " + server.name + ", " + cur_dt()
                log("INFO", "Searching for " + queue_name + "on server " + server.name + "...")
                jms_runtime = server.getJMSRuntime()
                jms_servers = jms_runtime.getJMSServers()
                for jms_server in jms_servers:
                    destinations = jms_server.getDestinations()
                    for dest in destinations:
                        name = get_queue_name(dest.name)
                        if queue_name == name:
                            report.append(("Queue name", name))
                            report.append(("Queue full name", dest.name))
                            report.append(("Messages Current Count", dest.messagesCurrentCount))
                            report.append(("Messages Received Count", dest.messagesReceivedCount))
                            report.append(("Messages Pending Count", dest.messagesPendingCount))
                            report.append(("Messages High Count", dest.messagesHighCount))
                            report.append(("Consumers Current Count", dest.consumersCurrentCount))

                            if int(dest.messagesCurrentCount):
                                # Get information about first and last messages
                                cursor = dest.getMessages("", 0)
                                cursor_size = dest.getCursorSize(cursor)
                                messages = dest.getNext(cursor, cursor_size)
                                if cursor_size > 1:
                                    msg_indexes = [0, cursor_size - 1]
                                else:
                                    msg_indexes = [0]
                                for i in msg_indexes:
                                    message = messages[i]
                                    jms_msg_info = JMSMessageInfo(message)
                                    wlmsg = jms_msg_info.getMessage()
                                    report.append(("", ""))
                                    if i == 0:
                                        report.append(("First message..........", ""))
                                    else:
                                        report.append(("Last message...........", ""))
                                    report.append(("JMSMessageID", wlmsg.getJMSMessageID()))
                                    loc_time = localtime(Double(wlmsg.getJMSTimestamp()//1000))
                                    jms_timestamp = strftime('%Y-%m-%d %H:%M:%S', loc_time)
                                    report.append(("JMSTimestamp", jms_timestamp))
                                    report.append(("PayloadSize", wlmsg.getPayloadSize()))
                                    report.append(("JMSExpiration", wlmsg.getJMSExpiration()))
                                    report.append(("JMSRedelivered", wlmsg.getJMSRedelivered()))
                                    report.append(("JMSRedeliveryLimit", wlmsg.getJMSRedeliveryLimit()))
                                dest.closeCursor(cursor)
                if report:
                    create_report(report_title, report, col_names, is_sorted=False)
        else:
            log("WARNING", "No servers were found at " + url + ". Terminating the script...")
        log("INFO", "get_queue_info completed.")
    
    except (WLSTException, ValueError, NameError, Exception, AttributeError, TypeError), e:
        log("ERROR", str(e))


def create_report(report_title, report, col_names, is_sorted):
    """ This function creates a tabular report with left or right text adjustment depending on the content data type.
    Input parameters:
        report_title - A string that will serve as the report title
        report - a table (a 2D list) of data of any type (that can be cast to string)
        col_names - a list of the column names, comma separated and wrapped into []. 
            Add '#' to the column name that will contain only numbers for right adjustment
            Example: ["ColTxt1", "ColNum1#", "ColTxt2"]
        is_sorted - Boolean. True - sort report rows, False - do not sort.
    """

    # Convert all values to strings
    report_str = []
    for row in report:
        row = [str(item) for item in row]
        report_str.append(row)
    
    if is_sorted:
        report_str.sort()
    
    # Remove "#" from headers and add to the beginning of the report
    report_str.insert(0, [col_name.replace("#", "") for col_name in col_names])
    
    # Count max column widths
    col_widths = [max(map(len, col)) for col in zip(*report_str)]
    
    # Create an underline and add to report
    underline = ["=" * width for width in col_widths]
    report_str.insert(1, underline)  # Header upper border
    report_str.insert(0, underline)  # Header lower border
    report_str.append(underline)  # Bottom line

    # Adjust columns left or right
    # rjust/ljust accept only one parameter in ver 2, therefore this workaround.
    report_adj = []
    for row in report_str:
        rw = list(zip(row, col_widths))
        row_adj = []
        for x in range(len(rw)):
            # right-adjust strings of the columns with "#" in the header and left-adjust other columns
            if "#" in col_names[x]:  # The column contains numbers
                row_adj.append(rw[x][0].rjust(rw[x][1]))
            else:
                row_adj.append(rw[x][0].ljust(rw[x][1]))
        report_adj.append(" ".join(row_adj))

    # Print the report
    f.write("\n")
    print("")
    log_report(report_title)
    for row in report_adj:
        log_report(row)
    f.write("\n")
    print("")


def cur_dt():
    """
    This function returns current date time in %Y-%m-%d %H:%M:%S format, i.e. 2018-08-27 13:28:15
    """
    # time.time() returns error in rare cases, e.g. when failed to connect, so using GMT
    # d = strftime("%Y-%m-%d %H:%M:%S", gmtime(time.time() + 3600)) # Always show time using CET TZ (GMT + P1H)
    d = strftime("%Y-%m-%d %H:%M:%S", gmtime())  # Always show time using CET TZ (GMT + P1H)
    return d


def log(level, text):
    """
    Function log appends a log string "text" to the log file f in the format: "YYYY-MM-DD HH:mm:SS id#### [LEVEL] text"
    E.g. "2018-09-05 12:22:33 id0010 [INFO] Creating session"
    """
    f.write(cur_dt() + " " + ID + " [" + level + "] " + str(text) + "\n")
    print(cur_dt() + " [" + level + "] " + str(text))


def log_report(text):
    """
    Function log_report is used for printing a report line to the standard output and the log file f.
    E.g. "2018-09-05 12:22:33 id0010 [INFO] Creating session"
    :param text: string.
    """
    f.write(text + "\n")
    print(text)
    

def start_connect(function_name, is_connected):
    """
    This function connection to the given server if not yet connected.
    :type function_name: string. Name of the function that will be started. Used for logging.
    :type is_connected: bool. Connection status: True - connected, False otherwise
    :rtype: bool
    """
    log("INFO", "======================================================================")
    log("INFO", "Starting " + function_name + "...")
    if not is_connected:
        log("INFO", "Connecting to " + url + " as " + usrname + "...")
        connect(usrname, password, url)
        print("")
        log("INFO", "Connected to " + url + " as " + usrname + ".")
    is_connected = True
    return is_connected


def get_queue_name(name):
    """
    This function returns queue name from queueBean.Name
    E.g. "IntegrationJmsModule!IntegrationJmsServer@osb_server1@jmsQueue1" results in "jmsQueue1"
    :rtype: str
    """
    if "@" in name:
        queue_name = name.split("@")[-1]
    elif "!" in name:
        queue_name = name.split("!")[-1]
    else:
        queue_name = name
    return queue_name


def main():
    """
    The main function. Prompts to select an action and calls the corresponding function
    """
    try:
        is_connected = False
        while True:
            print("")
            print("[1] List queues without listeners")
            print("[2] List all queues with current messages")
            print("[3] List DMQ queues with current messages")
            print("[4] Delete messages from queues")
            print("[5] Delete queues")
            print("[6] Move messages from one JMS queue to another")
            print("[7] Get queue information")
            print("[8] Exit")
            print("")
            procedure = raw_input("[INPUT] Choose what you want to do from the list above: ")
            print("")
            if not procedure:
                print(cur_dt() + " [ERROR] Input cannot be empty. Please, enter a number from 1 to 8.")
            elif procedure == "1":
                is_connected = start_connect("list_queues_without_listeners", is_connected)
                list_queues_without_listeners()
            elif procedure == "2":
                is_connected = start_connect("list_all_queues_with_current_messages", is_connected)
                list_all_queues_with_current_messages()
            elif procedure == "3":
                is_connected = start_connect("list_dmq_queues_with_current_messages", is_connected)
                list_dmq_queues_with_current_messages()
            elif procedure == "4":
                is_connected = start_connect("delete_messages_from_queue", is_connected)
                delete_messages_from_queue()
            elif procedure == "5":
                is_connected = start_connect("delete_queues", is_connected)
                delete_queues()
            elif procedure == "6":
                is_connected = start_connect("move_messages", is_connected)
                move_messages()
            elif procedure == "7":
                is_connected = start_connect("get_queue_info", is_connected)
                get_queue_info()
            else:
                break
        f.close()
        disconnect()
        exit()
    except (WLSTException, ValueError, NameError, Exception, AttributeError, EOFError), e:
       log("ERROR", str(e))
       f.close()
       disconnect()
       raise


# Create a four digit random id left padded with zeros for logging
n = random.randint(1, 1000)
ID = "id" + str("%04d" % n)
        
# Name of the log file is derived from the name of the script
log_file = sys.argv[0].replace("py", "log")
f = open(log_file, "a")
print(cur_dt() + " [INFO] Output is sent to " + log_file + ". Log ID = " + ID)


main()

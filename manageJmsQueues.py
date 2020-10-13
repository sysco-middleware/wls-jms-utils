"""
The script contains a set of utilities for managing JMS queues on the Weblogic Server.
Available functionality:
    [0] Change connection. Connect to a different environment (load connection details from another property file).
    [1] List all queues. The report will also contain count of current messages and current consumers.
    [2] List queues without listeners. The report will also contain count of current messages and current consumers.
    [3] List all queues with current messages. The report will  contain count of current messages and current consumers.
    [4] List DMQ queues with current messages. The report will also contain count of current messages.
    [5] Delete messages from a given queue. Optionally, use filer to select a set of messages.
    [6] Delete queues. Input: One or several queue names separated by space.
    [7] Move messages from one queue (e.g. DMQ) to another.
    [8] Get the queue information:  Queue name, Messages Current Count, Messages Received Count as well as some basic
        information about the queue's first and last messages (size, timestamp, etc)
Examples of message filters that can be used for deleting or moving a set of messages: 
        1. JMS_BEA_State LIKE 'expired' - remove all messages with state 'expired'
        2. JMSTimestamp > '2018-05-03 17:47:53.728' - all messages older than 2018-05-03T17:47:53.728
        3. JMSXDeliveryCount > 0 - all messages that were redelivered at least once
        4. JMSType = 'car' AND weight > 2500 - messages with a message type of car and weight greater than 2500
See here for more examples of message selectors: https://docs.oracle.com/javaee/6/api/javax/jms/Message.html
Manual usage:
1. Execute: wlst manageJmsQueues.py -loadProperties manageJmsQueues_[ENV].properties
2. Select action 0-9 manually
Automatic/silent usage:
1. Execute: wlst manageJMSQueues.py [operation] [env] [par1, par2, ..., parn]
"""

import os
import os.path
import random
import re
import sys

import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import java.text.ParseException

from time import strftime, localtime
from java.io import File
from java.io import FileInputStream
from java.util import Properties
from javax.jms import ObjectMessage
from javax.jms import TextMessage
from weblogic.jms.extensions import JMSMessageInfo


def main():
    """
    The main function. Prompts to select an action and calls the corresponding function.
    Stays in the loop until the exit is selected by the user (for non-standalone calls).
    """
    keep_main_loop = True
    is_connected = False
    connection_info = {"is_connected": is_connected, "env": env, "url": url, "username": username, "password": password}
    
    # Choose and environment and make a connection to it
    while not connection_info["is_connected"]:
        connection_info = connect_wls(connection_info)
        is_connected = connection_info["is_connected"]
        if is_standalone and not is_connected:
            f.close()
            disconnect()
            exit()
        elif not is_connected:
            connection_info["env"] = ""

    while keep_main_loop:

        if is_standalone:
            if len(sys.argv) > 1:
                procedure = sys.argv[1]
            else:
                log("ERROR", "Incomplete/incorrect list of parameters.")
                f.close()
                disconnect()
                exit()
            keep_main_loop = False
        else:
            if connection_info["env"]:
                cur_con_status = "currently connectred to " + connection_info["env"]
            else:
                cur_con_status = "currently not connected"
            print("")
            print("[0] Change environment (" + cur_con_status + ")")
            print("[1] List all queues")
            print("[2] List queues without listeners")
            print("[3] List all queues with current messages")
            print("[4] List DMQ queues with current messages")
            print("[5] Delete messages from queue")
            print("[6] Delete queues")
            print("[7] Move messages from one JMS queue to another")
            print("[8] Get queue information")
            print("[9] Exit")
            print("")
            while True:
                procedure = raw_input("[INPUT] Choose what you want to do from the list above: ")
                print("")
                if not procedure:
                    print(cur_dt() + " [ERROR] Input cannot be empty. Please, enter a number from 0 to 9.")
                else:
                    break
        try:
            if procedure == "0":
                connection_info["env"] = ""
                connection_info = connect_wls(connection_info)
            elif procedure == "1" or procedure == "list_all_queues":
                connection_info = start_connect(
                    "list_all_queues", connection_info)
                list_all_queues(connection_info)
            elif procedure == "2" or procedure == "list_queues_without_listeners":
                connection_info = start_connect(
                    "list_queues_without_listeners", connection_info)
                list_queues_without_listeners(connection_info)
            elif procedure == "3" or procedure == "list_all_queues_with_current_messages":
                connection_info = start_connect(
                    "list_all_queues_with_current_messages", connection_info)
                list_all_queues_with_current_messages(connection_info)
            elif procedure == "4" or procedure == "list_dmq_queues_with_current_messages":
                connection_info = start_connect(
                    "list_dmq_queues_with_current_messages", connection_info)
                list_dmq_queues_with_current_messages(connection_info)
            elif procedure == "5" or procedure == "delete_messages_from_queue":
                connection_info = start_connect(
                    "delete_messages_from_queue", connection_info)
                delete_messages_from_queue(connection_info)
            elif procedure == "6" or procedure == "delete_queues":
                connection_info = start_connect("delete_queues", connection_info)
                delete_queues(connection_info)
            elif procedure == "7" or procedure == "move_messages":
                connection_info = start_connect("move_messages", connection_info)
                move_messages(connection_info)
            elif procedure == "8" or procedure == "get_queue_info":
                connection_info = start_connect("get_queue_info", connection_info)
                get_queue_info(connection_info)
            elif procedure == "9":
                break
            else:
                log("ERROR", "Unknown procedure number: " + procedure + ". Try again.")
        except:
            log("ERROR", str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1]))
            disconnect()
            is_connected = False
            connection_info["is_connected"] = is_connected

    f.close()
    disconnect()
    exit()


def list_all_queues(connection_info):
    """
    This function lists all queues available on servers.
    The report will also contain count of current messages and current consumers.
    Automatic usage:
        wlst manageJmsQueues.py list_all_queues [env]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("WARNING", "No servers were found at " +
                connection_info["url"] + ". Terminating the script...")
            return

        report = []
        for server in servers:
            log("INFO", "Searching queues on server " + server.name + "...")
            jms_runtime = server.getJMSRuntime()
            jms_servers = jms_runtime.getJMSServers()
            for jms_server in jms_servers:
                destinations = jms_server.getDestinations()
                for dest in destinations:
                    cons_cur_cnt = dest.consumersCurrentCount
                    name = dest.name
                    msg_cur_cnt = dest.messagesCurrentCount
                    msg_pnd_cnt = dest.messagesPendingCount
                    report.append([name, cons_cur_cnt, msg_cur_cnt, msg_pnd_cnt])
        # Create report
        report_title = "REPORT: LIST OF ALL QUEUES, " + \
                       parse_url(connection_info["url"])["hostname"] + " (" + connection_info["env"] + "), " + cur_dt()
        col_names = ("QUEUE_NAME", "CUR_CONS", "CUR_MSG", "PEND_MSG")
        create_report(report_title, report, col_names, is_sorted=True, is_total=True)
        log("INFO", "list_all_queues completed.")
    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def list_queues_without_listeners(connection_info):
    """
    This function lists queues without listeners, i.e. having consumerCurrentCount = 0. 
    The report will also contain count of current messages and current consumers.
    Automatic usage:
        wlst manageJmsQueues.py list_queues_without_listeners [env]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("WARNING", "No servers were found at " +
                connection_info["url"] + ". Terminating the script...")
            return

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
        report_title = "REPORT: LIST OF QUEUES WITHOUT LISTENERS, " + \
                       parse_url(connection_info["url"])["hostname"] + " (" + connection_info["env"] + "), " + cur_dt()
        col_names = ("QUEUE_NAME", "CUR_CONS", "CUR_MSG")
        create_report(report_title, report, col_names, is_sorted=True, is_total=True)
        log("INFO", "list_queues_without_listeners completed.")

    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def list_all_queues_with_current_messages(connection_info):
    """
    This function lists all queues with current and/or pending messages (i.e. queues with messagesCurrentCount > 0.
    The report will also contain count of current and pending messages as well as count of current consumers.
    Automatic usage:
        wlst manageJmsQueues.py list_all_queues_with_current_messages [env]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("WARNING", "No servers were found at " +
                connection_info["url"] + ". Terminating the script...")
            return

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
                    msg_pen_cnt = dest.messagesPendingCount
                    if msg_cur_cnt > 0 or msg_pen_cnt > 0:
                        cons_cur_cnt = dest.consumersCurrentCount
                        report.append(
                            [name, cons_cur_cnt, msg_cur_cnt, msg_pen_cnt])
        # Create report
        report_title = "REPORT: LIST OF QUEUES WITH CURRENT MESSAGES, " + \
                       parse_url(connection_info["url"])["hostname"] + " (" + connection_info["env"] + "), " + cur_dt()
        col_names = ("QUEUE_NAME", "CUR_CONS", "CUR_MSG", "PEND_MSG")
        create_report(report_title, report, col_names, is_sorted=True, is_total=True)
        log("INFO", "list_all_queues_with_current_messages completed.")

    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def list_dmq_queues_with_current_messages(connection_info):
    """
    This function lists DMQ queues with current messages,
    i.e. with name having "dmq" in the name and where messagesCurrentCount > 0.
    The report will also contain count of current messages and current consumers.
    Automatic usage:
        wlst manageJmsQueues.py list_dmq_queues_with_current_messages [env]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """
    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("WARNING", "No servers were found at " +
                parse_url(connection_info["url"])["hostname"] + ". Terminating the script...")
            return

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
                        report.append([name, msg_cur_cnt])
        # Create report
        report_title = "REPORT: LIST OF DMQs WITH CURRENT MESSAGES, " + parse_url(connection_info["url"])[
            "hostname"] + " (" + connection_info["env"] + "), " + cur_dt()
        col_names = ("QUEUE_NAME", "CUR_MSG")
        create_report(report_title, report, col_names, is_sorted=True, is_total=True)
        log("INFO", "list_dmq_queues_with_current_messages completed.")

    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))


def delete_messages_from_queue(connection_info):
    """
    This function deletes all messages from a given queue.
    Automatic usage:
        wlst manageJmsQueues.py delete_messages_from_queue [env] [queue_name] [filter]
    """
    if is_standalone:
        if len(sys.argv) > 3:
            queue_name = sys.argv[3]
        else:
            log("ERROR", "Incomplete/incorrect list of parameters.")
            f.close()
            disconnect()
            exit()
    else:
        while True:
            queue_name = raw_input(
                "[INPUT] Enter queue name (e.g. WLMsgQueueName): ")
            if not queue_name:
                print(
                    cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid name")
                continue
            else:
                break
        
        if len(sys.argv) > 4:
            msg_filter = sys.argv[4:]
        else:
            msg_filter = raw_input("[INPUT] Enter message filter (e.g. JMSTimestamp > \'2019-01-01 00:00:00.000\') or leave blank: ")
            msg_filter.strip()
            msg_filter = parse_filter(msg_filter)

        cursor = ""
        if msg_filter:
            log("INFO", "The following message filter will be applied:  " + msg_filter)
        else:
            msg_filter = ""
            log("INFO", "No filters will be applied")

    queue_name = queue_name.strip()

    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("ERROR", "No servers were found at " +
                parse_url(connection_info["url"])["hostname"] + ". Terminating the script...")
            return

        is_dest_found = False
        dest_info = parse_destination_name(queue_name)

        for server in servers:
            if "server" in dest_info and dest_info["server"] != server.name:
                continue
            is_dest_found_srv = False
            log("INFO", "Searching queues on server " + server.name + "...")
            jms_runtime = server.getJMSRuntime()
            jms_servers = jms_runtime.getJMSServers()

            for jms_server in jms_servers:
                jms_server_name = jms_server.name
                if "jms_server" in dest_info and dest_info["jms_server"] != jms_server_name.split("@")[0]:
                    continue
                destinations = jms_server.getDestinations()

                for dest in destinations:
                    if "jms_module" in dest_info:
                        dest_name = dest.name
                    else:
                        dest_name = get_queue_name(dest.name)
                    if queue_name == dest_name:
                        print(cur_dt() + " [INFO] =====================")
                        is_dest_found_srv = True
                        is_dest_found = True
                        msg_cur_cnt = dest.messagesCurrentCount
                        cons_cur_cnt = dest.consumersCurrentCount
                        if msg_filter:
                            cursor = dest.getMessages(msg_filter, 600)
                            cursor_size = dest.getCursorSize(cursor)
                            msg_to_del_cnt = cursor_size
                        else:
                            msg_to_del_cnt = msg_cur_cnt
                        log("INFO", "Name: " + dest_name + ", Current consumers: " + str(cons_cur_cnt) + 
                            ", Current messages: " + str(msg_cur_cnt) + ", Messages to delete: " + str(msg_to_del_cnt))
                        if msg_to_del_cnt > 0:
                            if not is_standalone:
                                del_msgs_choice = raw_input(
                                    "[INPUT] Do you want to delete messages from this queue, Y/N [Y]? ")
                            else:
                                del_msgs_choice = "Y"
                            print("")
                            if del_msgs_choice.upper() == "Y" or del_msgs_choice.strip() == "":
                                log("INFO", "Deleting " +
                                    str(msg_to_del_cnt) + " messages...")
                                msg_deleted_cnt = dest.deleteMessages(msg_filter)
                                
                                # Check current messages after delete
                                if msg_deleted_cnt == msg_to_del_cnt:
                                    log("INFO", "Successfully deleted " +
                                        str(msg_deleted_cnt) + " out of " + str(msg_to_del_cnt) + " messages")
                                else:
                                    log("WARNING", "Deleted " + str(msg_deleted_cnt) + " out of "
                                        + str(msg_to_del_cnt) + " messages. Try to repeat the procedure to delete the remaining messages.")
                            else:
                                log("INFO", "Skipping as per user prompt...")
                        else:
                            log("INFO", "The queue is empty. Skipping...")
                        if cursor:
                            dest.closeCursor(cursor)
                            cursor = ""

                if is_dest_found_srv:
                    break  # stop searching jms_servers on this server

        if not is_dest_found:
            log("ERROR", "The queue was not found.")
        log("INFO", "delete_messages_from_queue completed.")

    except (WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))
        if cursor:
            dest.closeCursor(cursor)
            cursor = ""


def delete_queues(connection_info):
    """
    This function deletes JMS queues from the given list of queues.
    The list must be space separated and must contain at least one queue name, i.e. "WLMsgQueueName1"
    For automatic calls, list of queues follows the env: 
        wlst manageJmsQueue delete_queues [env] [Q1 [Q2 Qn]]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """
    if is_standalone:
        if len(sys.argv) > 3:
            queue_names = sys.argv[3:]
        else:
            log("ERROR", "Incomplete list of attributes.")
            f.close()
            disconnect()
            exit()
    else:
        while True:
            queue_names = raw_input(
                "[INPUT] Enter names of the queues separated by a space: ")
            if not queue_names:
                print(cur_dt()
                      + "[ERROR] List of queue names cannot be empty and must contain at least one name")
                continue
            else:
                queue_names = queue_names.split(" ")
                break
    print("")
    log("INFO", "Queues to delete: " + str(queue_names))
    report = []

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
                cd('/JMSSystemResources/' + jms_module_name +
                   '/JMSResource/' + jms_module_name)
                # Search among uniform distributed queues
                queue_bean = cmo.lookupUniformDistributedQueue(queue_name)
                if queue_bean:
                    cnt += 1
                    print("")
                    log("INFO", "UniformDistributedQueue '" + queue_name + "' was found in JMS module '"
                        + jms_module_name + "'")

                    if not is_standalone:
                        del_queue_choice = raw_input(
                            "[INPUT] Do you want to delete this queue, Y/N [Y]? ")
                    else:
                        del_queue_choice = "Y"

                    if del_queue_choice.upper() == "Y" or del_queue_choice.strip() == "":
                        cmo.destroyUniformDistributedQueue(queue_bean)
                        log("INFO", "UniformDistributedQueue '" +
                            queue_name + "' deleted.")
                        report.append(
                            ["UniformDistributedQueue", queue_name, "Deleted"])
                    else:
                        report.append(["UniformDistributedQueue",
                                       queue_name, "Skipped by user"])
                else:
                    # Search among non-distributed queues
                    queue_bean = cmo.lookupQueue(queue_name)
                    if queue_bean:
                        cnt += 1
                        log("INFO", "Queue '" + queue_name +
                            "' was found in JMS module '" + jms_module_name + "'")

                        if not is_standalone:
                            del_queue_choice = raw_input(
                                "[INPUT] Do you want to delete this queue, Y/N [Y]? ")
                        else:
                            del_queue_choice = "Y"

                        if del_queue_choice.upper() == "Y" or del_queue_choice.strip() == "":
                            cmo.destroyQueue(queue_bean)
                            log("INFO", "Queue " + queue_name + " deleted.")
                            report.append(["Queue", queue_name, "Deleted"])
                        else:
                            report.append(
                                ["Queue", queue_name, "Skipped by user"])
                    else:
                        # Search among foreign destinations
                        frn_srvs = cmo.getForeignServers()
                        for frn_srv in frn_srvs:
                            frn_dest_bean = frn_srv.lookupForeignDestination(
                                queue_name)
                            if frn_dest_bean:
                                cnt += 1
                                cd('/JMSSystemResources/' + jms_module_name + '/JMSResource/' + jms_module_name
                                   + '/ForeignServers/' + frn_srv.name)
                                log("INFO", "ForeignDestination '" + queue_name + "' was found in JMS module '"
                                    + jms_module_name + "'")

                                if not is_standalone:
                                    del_queue_choice = raw_input(
                                        "[INPUT] Do you want to delete this queue, Y/N [Y]? ")
                                else:
                                    del_queue_choice = "Y"

                                if del_queue_choice.upper() == "Y" or del_queue_choice.strip() == "":
                                    cmo.destroyForeignDestination(
                                        frn_dest_bean)
                                    log("INFO", "ForeignDestination '" +
                                        queue_name + "' was deleted successfully.\n")
                                    report.append(
                                        ["ForeignDestination", queue_name, "Deleted"])
                                else:
                                    report.append(
                                        ["ForeignDestination", queue_name, "Skipped by user"])
                        continue
                    # TODO: Add topics

            if cnt == 0:
                log("INFO", "'" + queue_name +
                    "' was not found in any JMS module.")
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
        report_title = "REPORT: DELETE QUEUES, " + \
                       parse_url(connection_info["url"])["hostname"] + " (" + connection_info["env"] + "), " + cur_dt()
        col_names = ("OBJECT_TYPE", "OBJECT_NAME", "STATUS")
        create_report(report_title, report, col_names, is_sorted=True, is_total=False)
        log("INFO", "delete_queues completed.")

    except (WLSTException, ValueError, NameError, Exception, CommunicationException), e:
        log("ERROR", str(e))
        log("INFO", "Undoing changes and canceling edit session...")
        undo("true", "y")
        cancelEdit("y")
        if report:
            report_title = "REPORT: DELETE QUEUES, " + \
                           parse_url(connection_info["url"])["hostname"] + " (" + connection_info[
                               "env"] + "), " + cur_dt()
            col_names = ("OBJECT TYPE", "OBJECT NAME", "STATUS")
            create_report(report_title, report, col_names, is_sorted=True, is_total=False)
        print("")


def move_messages(connection_info):
    """
    This function moves messages from one queue (e.g. DMQ) to another.
    Input: Names of the source queue (e.g. WLMsgRampe_Vare_PRMS_dmq) and the target queue (e.g. WLMsgRampe_Vare_PRMS).
    Usage for automatic calls:
        wlst manageJmsQueue move_messages [env] [Qsrc] [Qtgt] [filter]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """
    # Assign source and target queues
    if is_standalone:
        if len(sys.argv) > 4:
            q_src_name = sys.argv[3]
            q_trg_name = sys.argv[4]
        else:
            log("ERROR", "Incomplete/incorrect list of parameters.")
            f.close()
            disconnect()
            exit()
    else:
        while True:
            q_src_name = raw_input("[INPUT] Enter name of the source queue: ")
            if not q_src_name:
                print(
                    cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid queue name.")
                continue
            else:
                break
        q_src_name = q_src_name.strip()

        while True:
            q_trg_name = raw_input("[INPUT] Enter name of the target queue: ")
            if not q_trg_name:
                print(
                    cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid queue name.")
                continue
            else:
                break
        q_trg_name = q_trg_name.strip()
        print("")
    log("INFO", "Source queue: " + q_src_name + ", Target queue: " + q_trg_name)

    # Assign filter
    if len(sys.argv) > 5:
        msg_filter = sys.argv[5:]
    else:
        msg_filter = raw_input("[INPUT] Enter message filter (e.g. JMSTimestamp > \'2019-01-01 00:00:00.000\') or leave blank: ")
        msg_filter.strip()
        msg_filter = parse_filter(msg_filter)
    cursor = ""
    if msg_filter:
        log("INFO", "The following message filter will be applied:  " + msg_filter)
    else:
        msg_filter = ""
        log("INFO", "No filters will be applied")

    try:
        serverRuntime()
        print("")

        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("ERROR", "No servers were found at " +
                parse_url(connection_info["url"])["hostname"] + ". Terminating the script...")
            return

        report = []
        q_beans_list = []
        q_msg_total = 0
        q_msg_move_total = 0
        targeted_jms_srv_name = ""
        q_src_found = False
        q_trg_found = False
        for server in servers:
            q_src_found_srv = False
            q_trg_found_srv = False
            log("INFO", "Processing server: " + server.name + "...")
            jms_runtime = server.getJMSRuntime()
            jms_servers = jms_runtime.getJMSServers()
            for jms_server in jms_servers:
                jms_server_name = jms_server.name
                # Remove server part if present (e.g. "jmsSrv1@Srv1" -> "jmsSrv1")
                if "@" in jms_server_name:
                    jms_server_name = jms_server_name.split("@")[0]
                if targeted_jms_srv_name and jms_server_name != targeted_jms_srv_name:
                    continue  # when the JMS Server name is known from previous iteration, skip others
                print(
                    cur_dt() + " [INFO] Processing JMS Server: " + jms_server.name + "...")
                destinations = jms_server.getDestinations()
                for dest in destinations:
                    # TODO: Should be a better way.
                    queue_name = get_queue_name(dest.name)
                    if queue_name == q_src_name:
                        # log("INFO", "Source queue found: " + dest.name)
                        q_src_found = True
                        q_src_found_srv = True
                        q_src_bean = dest
                        q_src_bean_msg_cur_cnt = q_src_bean.messagesCurrentCount
                        if msg_filter:
                            cursor = dest.getMessages(msg_filter, 60)
                            cursor_size = dest.getCursorSize(cursor)
                            msg_to_move_cnt = cursor_size
                            dest.closeCursor(cursor)
                            cursor = ""
                        else:
                            msg_to_move_cnt = q_src_bean_msg_cur_cnt
                        q_msg_total = q_msg_total + q_src_bean_msg_cur_cnt
                        q_msg_move_total = q_msg_move_total + msg_to_move_cnt
                    if queue_name == q_trg_name:
                        # log("INFO", "Target queue found: " + dest.name)
                        q_trg_found = True
                        q_trg_found_srv = True
                        q_trg_bean = dest
                    if q_src_found_srv and q_trg_found_srv:
                        break  # stop processing queues on this JMS server
                if q_src_found_srv and q_trg_found_srv:
                    q_beans_list.append(
                        [q_src_bean, q_trg_bean, q_src_bean_msg_cur_cnt, msg_to_move_cnt])
                    report.append(
                        [server.name, jms_server_name, q_src_name, q_trg_name, q_src_bean_msg_cur_cnt, msg_to_move_cnt])
                    targeted_jms_srv_name = jms_server_name
                    break  # go to the next server

        if q_src_found and q_trg_found:
            report_title = "REPORT: QUEUES FOUND FOR MOVE MESSAGES, " + \
                           parse_url(connection_info["url"])["hostname"] + " (" + connection_info[
                               "env"] + "), " + cur_dt()
            col_names = ("SERVER", "JMS SERVER", "SOURCE QUEUE",
                         "TARGET QUEUE", "MSG_COUNT", "MSG_TO_MOVE_COUNT")
            create_report(report_title, report, col_names, is_sorted=True, is_total=True)
        elif q_src_found and not q_trg_found:
            log("ERROR", "Target queue was not found")
        elif not q_src_found and q_trg_found:
            log("ERROR", "Source queue was not found")
        else:
            log("ERROR", "Neither source nor target queue were not found")

        if q_msg_move_total == 0:
            log("INFO", "There are no messages to move.")

        if q_msg_move_total > 0 and not is_standalone:
            mv_msgs_choice = raw_input("[INPUT] Do you want to move messages from '" + q_src_name + "' to '"
                                       + q_trg_name + "', Y/N [Y]? ")

        if q_msg_move_total > 0 and (is_standalone or mv_msgs_choice.strip().upper() == "Y" or mv_msgs_choice.strip() == ""):
            for row in q_beans_list:
                msg_to_move_cnt = row[3]
                if msg_to_move_cnt > 0:
                    q_src_bean = row[0]
                    q_trg_bean = row[1]
                    log("INFO", "Moving " + str(msg_to_move_cnt) + " messages from '"
                        + q_src_bean.name + "'...")
                    
                    # Move messages
                    q_msg_moved_cnt = q_src_bean.moveMessages(msg_filter, q_trg_bean.getDestinationInfo())
                    
                    if q_msg_moved_cnt == msg_to_move_cnt:
                        log("INFO", "Successfully moved " + str(q_msg_moved_cnt) + " out of " + str(msg_to_move_cnt)
                            + " messages to '" + q_trg_bean.name + "'.")
                    else:
                        log("WARNING", "There are still current messages on '" + q_trg_bean.name
                            + "'. Repeat the procedure.")
        else:
            log("WARNING", "Operation canceled by the user.")

        log("INFO", "move_messages completed.")

    except (ServiceUnavailableException, WLSTException, ValueError, NameError, Exception), e:
        log("ERROR", str(e))
        if cursor:
            dest.closeCursor(cursor)
            cursor = ""
        if report:
            report_title = "REPORT: MOVE_MESSAGES , " + \
                           parse_url(connection_info["url"])["hostname"] + " (" + connection_info[
                               "env"] + "), " + cur_dt()
            col_names = ("SERVER", "JMS SERVER", "SOURCE QUEUE",
                         "TARGET QUEUE", "MSG_COUNT", "MSG_TO_MOVE_COUNT")
            create_report(report_title, report, col_names, is_sorted=True, is_total=True)
            f.write("\n")
        print("")


def get_queue_info(connection_info):
    """
    This function returns information on a given queue.
    Automatic usage:
        wlst manageJmsQueues.py get_queue_info [env] [queue_name]
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    """

    if is_standalone:
        if len(sys.argv) > 3:
            queue_name = sys.argv[3]
        else:
            log("ERROR", "Incomplete list of attributes.")
            f.close()
            disconnect()
            exit()
    else:
        while True:
            queue_name = raw_input(
                "[INPUT] Enter a queue name (e.g. WLMsgQueueName): ")
            if not queue_name:
                print(
                    cur_dt() + " [ERROR] Queue name cannot be empty. Please, enter a valid name")
                continue
            else:
                break

    queue_name = queue_name.strip()
    log("INFO", "Entered queue name: " + queue_name)

    try:
        servers = domainRuntimeService.getServerRuntimes()
        if len(servers) == 0:
            log("WARNING", "No servers were found at " +
                parse_url(connection_info["url"])["hostname"] + ". Terminating the script...")
            return

        col_names = ("PROPERTY", "VALUE")
        for server in servers:
            report = []
            report_title = "REPORT: INFORMATION ON QUEUE " + \
                           queue_name + ", " + server.name + \
                           " (" + connection_info["env"] + ") " + cur_dt()
            log("INFO", "Searching for " + queue_name +
                " on server " + server.name + "...")
            jms_runtime = server.getJMSRuntime()
            jms_servers = jms_runtime.getJMSServers()
            for jms_server in jms_servers:
                destinations = jms_server.getDestinations()
                for dest in destinations:
                    name = get_queue_name(dest.name)
                    if queue_name == name:
                        report.append(("Queue name", name))
                        report.append(("Queue full name", dest.name))
                        report.append(("Messages Current Count",
                                       dest.messagesCurrentCount))
                        report.append(("Messages Received Count",
                                       dest.messagesReceivedCount))
                        report.append(("Messages Pending Count",
                                       dest.messagesPendingCount))
                        report.append(
                            ("Messages High Count", dest.messagesHighCount))
                        report.append(("Consumers Current Count",
                                       dest.consumersCurrentCount))

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
                                    report.append(
                                        ("First message..........", ""))
                                else:
                                    report.append(
                                        ("Last message...........", ""))
                                report.append(
                                    ("JMSMessageID", wlmsg.getJMSMessageID()))
                                loc_time = localtime(
                                    Double(wlmsg.getJMSTimestamp() // 1000))
                                jms_timestamp = strftime(
                                    '%Y-%m-%d %H:%M:%S', loc_time)
                                report.append(("JMSTimestamp", jms_timestamp))
                                report.append(
                                    ("PayloadSize", wlmsg.getPayloadSize()))
                                report.append(
                                    ("JMSExpiration", wlmsg.getJMSExpiration()))
                                report.append(
                                    ("JMSRedelivered", wlmsg.getJMSRedelivered()))
                                report.append(
                                    ("JMSRedeliveryLimit", wlmsg.getJMSRedeliveryLimit()))
                            dest.closeCursor(cursor)
            if report:
                create_report(report_title, report, col_names, is_sorted=False, is_total=False)
            else:
                log("WARNING", "Queue was not found.")
        log("INFO", "get_queue_info completed.")

    except (WLSTException, ValueError, NameError, Exception, AttributeError, TypeError), e:
        log("ERROR", str(e))


def create_report(report_title, report, col_names, is_sorted, is_total):
    """ This function prints a tabular report with left or right text adjustment depending on the content data type.
    :type report_title: str. The title of the report
    :type report: list. A table (a 2D list) of data of any type (that can be cast to string)
    :type col_names: list. A list of the column names, comma separated and wrapped into [].
    :type is_sorted: bool. True - sort report rows, False - do not sort.
    :type is_total: bool. If True, a Total row will be added to the report.
    """
    if not report:
        log("INFO", "The search returned no results.")
        return

    # Find columns with numbers for sorting and summarizing
    numeric_columns = []
    columns = tuple(zip(*report))
    for index, column in enumerate(columns):
        if is_all_int(column):
            numeric_columns.append(index)

    # Convert all values to strings
    report_str = []
    for row in report:
        row = [str(item) for item in row]
        report_str.append(row)

    if is_sorted:
        report_str.sort()

    # Create Totals row if required
    if is_total:
        totals_row = []
        for i in range(len(columns)):
            if i in numeric_columns:
                totals_row.append(sum(columns[i]))
            else:
                totals_row.append("")
        if not totals_row[0]:
            report_rows_count = len(columns[0])
            totals_row[0] = "TOTAL (" + str(report_rows_count) + ")"
        totals_row = [str(item) for item in totals_row]
        report_str.append(totals_row)

    report_str.insert(0, col_names)

    # Count max column widths
    col_widths = [max(map(len, col)) for col in zip(*report_str)]

    # Create an underline and add to report
    underline_thick = ["=" * width for width in col_widths]
    underline_thin = ["-" * width for width in col_widths]
    report_str.insert(1, underline_thin)  # Header lower border
    report_str.insert(0, underline_thick)  # Header upper border
    report_str.append(underline_thick)  # Bottom line
    if is_total:
        report_str.insert(len(report_str) - 2, underline_thin)  # Separate totals from the rest

    # Adjust columns left or right
    # rjust/ljust accept only one parameter in Jython 2.2.1, therefore this workaround.
    report_adj = []
    for row in report_str:
        rw = list(zip(row, col_widths))
        row_adj = []
        for x in range(len(rw)):
            # right-adjust strings of the numeric columns and left-adjust other columns
            if x in numeric_columns:  # The column contains numbers
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
    This function returns current local date time in %Y-%m-%d %H:%M:%S %Z format, i.e. 2018-08-27 13:28:15 CEST
    :rtype local_dt: str
    """
    local_dt = strftime("%Y-%m-%d %H:%M:%S %Z", localtime())
    return local_dt


def log(level, text):
    """
    Function log appends a log string "text" to the log file f in the format: "YYYY-MM-DD HH:mm:SS id#### [LEVEL] text"
    E.g. "2018-09-05 12:22:33 id0010 [INFO] Creating session"
    :type level: str. INFO, WARNING, ERROR
    :type text: str. The text of the log message
    """
    f.write(cur_dt() + " " + ID + " [" + level + "] " + str(text) + "\n")
    print(cur_dt() + " [" + level + "] " + str(text))


def log_report(text):
    """
    Function log_report is used for printing a report line to the standard output and the log file f.
    E.g. "2018-09-05 12:22:33 id0010 [INFO] Creating session"
    :type text: str
    """
    f.write(text + "\n")
    print(text)


def start_connect(function_name, connection_info):
    """
    This function connection to the given server if not yet connected.
    :type function_name: string. Name of the function that will be started. Used for logging.
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    :rtype: dict
    """
    log("INFO", "======================================================================")
    log("INFO", "Starting " + function_name + " in " + connection_info["env"] + "...")

    if not connection_info["is_connected"] or not connection_info["url"]:
        connection_info = connect_wls(connection_info)
    return connection_info


def connect_wls(connection_info):
    """
    This function 
        1. Prompts for an environment among those available,
        2. Reads the properties from the corresponding property file
        3. Makes a connection to the given environment
    :type connection_info: dict. Connection information: is_connected, env, url, username, password
    :rtype: dict
    """
    if connection_info["is_connected"]:
        disconnect()

    env = connection_info["env"]
    if not env:
        print("")
        print("Available environments: ")
        print("----------------------- ")
        env_list = list(prop_env_file.keys())
        env_list.sort()
        for env in env_list:
            print(env)
        print("")

        while True:
            env = raw_input("[INPUT] Choose an environment from the list above: ")
            if env in prop_env_file:
                break
            else:
                print(cur_dt() + " [WARNING] The provided environment name is not found in the list. Try again.")

    # Check that env was provided when starting the script as standalone
    if env in prop_env_file:
        prop_file_name = prop_env_file[env]
    else:
        log("ERROR", "Property file for the environment " + env + " was not found.  ")
        f.close()
        exit()

    # Read properties from the propery file
    in_stream = FileInputStream(prop_file_name)
    prop_file = Properties()
    prop_file.load(in_stream)
    url = prop_file.getProperty("url")
    username = prop_file.getProperty("usrname")
    password = prop_file.getProperty("password")

    log("INFO", "Trying to connect to " + url + " as " + username + "...")
    try:
        connect(username, password, url)
        is_connected = True
        connection_info = {"is_connected": is_connected, "env": env, "url": url, "username": username, "password": password}
    except:
        log("ERROR", str(sys.exc_info()[0]) + " " + str(sys.exc_info()[1]))
        is_connected = False
        connection_info = {"is_connected": is_connected, "env": env, "url": url, "username": username, "password": password}
    return connection_info


def get_queue_name(name):
    """
    This function returns queue name from queueBean.Name
    E.g. "IntegrationJmsModule!IntegrationJmsServer@osb_server1@jmsQueue1" results in "jmsQueue1"
    :type name: str
    :rtype: str
    """
    if "@" in name:
        queue_name = name.split("@")[-1]
    elif "!" in name:
        queue_name = name.split("!")[-1]
    else:
        queue_name = name
    # For jndi-s like this: UMSJMSSystemResource!UMSJMSServer_auto_1@dist_OraSDPM/Queues/OraSDPMEngineCmdQ_auto
    if "/" in queue_name:
        queue_name = queue_name.split("/")[-1]

    return queue_name


def parse_destination_name(name):
    """
    This function parses destinationBean.Name and returns a dict with parsed values:
    {"jms_module": jms_module, "jms_server": jms_server, "server": server, "jndi_name": jndi_name}
    :type name: str
    :rtype: dict
    """
    search_result = re.search("(.+)!(.+)@(.+)@(.+)", name)  # [jms_module]![jms_server]@[server]@[destination]
    if search_result:
        jms_module = search_result.group(1)
        jms_server = search_result.group(2)
        server = search_result.group(3)
        jndi_name = search_result.group(4)
        dest_info = {"jms_module": jms_module, "jms_server": jms_server, "server": server, "jndi_name": jndi_name}

        return dest_info

    search_result = re.search("(.+)!(.+)@(.+)", name)  # [jms_module]![jms_server]@[destination]
    if search_result:
        jms_module = search_result.group(1)
        jms_server = search_result.group(2)
        jndi_name = search_result.group(3)
        dest_info = {"jms_module": jms_module, "jms_server": jms_server, "jndi_name": jndi_name}

        return dest_info

    search_result = re.search("(.+)!(.+)", name)  # [jms_module]![destination]
    if search_result:
        jms_module = search_result.group(1)
        jndi_name = search_result.group(2)
        dest_info = {"jms_module": jms_module, "jndi_name": jndi_name}

        return dest_info

    dest_info = {"jndi_name": name}

    return dest_info


def parse_url(url):
    """
    This function parses url and returns a dict with parsed values:
        {"protocol": protocol, "hostname": hostname, "port": port, "path": path}
    :type: url: str
    :rtype: dict
    """
    search_result = re.search("(\w+)://(.+):(\d+)", url)  # [protocol]://[hostname]:[port]
    if search_result:
        protocol = search_result.group(1)
        hostname = search_result.group(2)
        port = search_result.group(3)
        return {"protocol": protocol, "hostname": hostname, "port": port}

    search_result = re.search("(\w+)://(.+):(\d+)/(\w*)", url)  # [protocol]://[hostname]:[port]/[path]
    if search_result:
        protocol = search_result.group(1)
        hostname = search_result.group(2)
        port = search_result.group(3)
        path = search_result.group(4)
        return {"protocol": protocol, "hostname": hostname, "port": port, "path": path}

    search_result = re.search("(\w+)://(.+)/(\w+)", url)  # [protocol]://[hostname]/[path]
    if search_result:
        protocol = search_result.group(1)
        hostname = search_result.group(2)
        path = search_result.group(3)
        return {"protocol": protocol, "hostname": hostname, "path": path}

    return {"hostname": url}


def is_all_int(test_array):
    """
    Could not make 'all(isinstance(x, int) for x in list)' work in Jython, therefor this function.
    :rtype: bool
    """
    for i in test_array:
        if not isinstance(i, (int, long)):
            return False
    return True


def get_env_prop_file():
    """ 
    The function creates a dictionary of pairs of {"env": "env_property_file_name"}.
    The current directory is scanned for files with extension ".properties".
    The key part that represents the environment is received from the name of the property file 
    assuming that the environment part comes after last "_" and before the extension (.properties).
    Example of a property file name: "manageJmsQueues_DEV.properties".
    The value contains the name of the property file for the corresponding environment (key).
    :rtype: dict
    """
    prop_env_file = {}
    for f_name in os.listdir(os.getcwd()):
        if f_name.endswith('.properties'):
            # remove the part after the dot
            prop_env = f_name.split(".")[0]
            if "_" in f_name:
                # Environment name is the text that goes after the last "_"
                prop_env = prop_env.split("_")[-1]
            prop_env_file[prop_env] = f_name
    return prop_env_file


def parse_filter(msg_filter):
    """ 
    The function converts a given timestamp in in the format yyyy-MM-dd HH:mm[:ss[.SSS]] into milliseconds
    :type: msg_filter: str
    :rtype: str
    """    
    # Search for timestamp in format "yyyy-MM-dd HH:mm:ss.SSS"
    search_result = re.search("(.*)[\'|\"]*(\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:\d{2}\.\d{3})", msg_filter)
    if search_result:
        filter_part1 = search_result.group(1)
        filter_part2 = search_result.group(2)
        msg_filter = filter_part1 + str(get_milliseconds(filter_part2))
        return msg_filter
    
    # Search for timestamp in format "yyyy-MM-dd HH:mm:ss"
    search_result = re.search("(.*)(\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:\d{2})", msg_filter)
    if search_result:
        filter_part1 = search_result.group(1)
        filter_part2 = search_result.group(2)
        msg_filter = filter_part1 + str(get_milliseconds(filter_part2 + ".000"))
        return msg_filter
    
    # Search for timestamp in format "yyyy-MM-dd HH:mm"
    search_result = re.search("(.*)(\d{4}-\d{2}-\d{2}.\d{2}:\d{2})", msg_filter)
    if search_result:
        filter_part1 = search_result.group(1)
        filter_part2 = search_result.group(2)
        msg_filter = filter_part1 + str(get_milliseconds(filter_part2 + ":00.000"))
        return msg_filter
    return msg_filter


def get_milliseconds(timestamp_str):
    """ 
    The function converts a given timestamp in in the format yyyy-MM-dd HH:mm:ss.SSS into milliseconds
    :type: timestamp: str
    :rtype: long
    """
    sdf = java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    date = sdf.parse(timestamp_str)
    timestampe_in_millisec = date.getTime()
    return timestampe_in_millisec


# Create a four digit random id left padded with zeros for logging
n = random.randint(1, 1000)
ID = "id" + str("%04d" % n)

# Name of the log file is derived from the name of the script
log_file = sys.argv[0].replace("py", "log")
print(cur_dt() + " [INFO] Output is sent to " + log_file + ". Log ID = " + ID)
f = open(log_file, "a")

prop_env_file = get_env_prop_file()

if len(sys.argv) > 1:
    is_standalone = True
else:
    is_standalone = False

if len(sys.argv) > 1:
    env = sys.argv[2]
else:
    env = ""
url = ""
username = ""
password = ""

main()
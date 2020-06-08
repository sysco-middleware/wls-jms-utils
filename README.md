# wls-jms-utils

The script contains a set of utilities for managing JMS queues on the Weblogic Server.

## Available functionality:
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

## Manual usage:
1. Execute: wlst manageJmsQueues.py -loadProperties manageJmsQueues_[ENV].properties
2. Select action 0-9 manually

## Automatic/silent usage:
1. Execute: wlst manageJMSQueues.py [operation] [env] [par1, par2, ..., parn]

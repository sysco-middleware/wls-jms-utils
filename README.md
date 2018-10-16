# wls-jms-utils

The script contains a set of utilities for managing JMS queues on the Weblogic Server.

## Available functionality:
    [1] List queues without listeners. The report will also contain count of current messages and current consumers.
    [2] List all queues with current messages. The report will  contain count of current messages and current consumers.
    [3] List DMQ queues with current messages. The report will also contain count of current messages.
    [4] Delete messages from a given queue.
    [5] Delete queues. Input: List of space separated queue names.
    [6] Move messages from one queue (e.g. DMQ) to another.
    [7] Get the queue information:  Queue name, Messages Current Count, Messages Received Count as well as some basic
        information about queues first and last messages (size, timestamp, etc)

## Usage:
1. Execute: wlst manageJmsQueues.py -loadProperties manageJmsQueues.properties
2. Select action

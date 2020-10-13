# wls-jms-utils

The script contains a set of utilities for managing JMS queues on the Weblogic Server.

Available functionality:
    [0] Change connection. Connect to a different environment (load connection details from another property file).
    [1] List all queues. The report will also contain count of current messages and current consumers.
    [2] List queues without listeners. The report will also contain count of current messages and current consumers.
    [3] List all queues with current messages. The report will contain count of current messages and current consumers.
    [4] List DMQ queues with current messages. The report will also contain count of current messages.
    [5] Delete messages from a given queue. Optionally, use filer to select a set of messages.
    [6] Delete queues. Input: one or several queue names separated by space.
    [7] Move messages from one queue (e.g. DMQ) to another (with or without message selector/filter).
    [8] Get the queue information:  Queue name, Messages Current Count, Messages Received Count as well as some basic
        information about the queue's first and last messages (size, timestamp, etc)

Manual usage (alternative 1, generic):
1. Add %ORACLE_HOME%/oracle_common/common/bin to the environmental variable PATH 
2. Execute: wlst manageJmsQueues.py -skipWLSModuleScanning
3. Select action 0-9

Manual usage (alternative 2, Windows):
1. Start manageJmsQueues.cmd
2. Select action 0-9

Automatic/silent usage:
1. Execute: wlst manageJmsQueues.py [operation] [env] [par1, par2, ..., parN] (see comments to the required function)

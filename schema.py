# column mappings
indexMapping = lambda l: {s:i for i,s in enumerate(l)}
columnIndex = {
    'jobEvents': indexMapping(
        ['time','missing info','job ID','event type','user',
        'scheduling class','job name','logical job name']
    ),
    'machineAttributes': indexMapping(
        ['time','machine ID','attribute name','attribute value','attribute deleted']
    ),
    'machineEvents': indexMapping(
        ['time','machine ID','event type','platform ID','CPUs','Memory']
    ),
    'taskConstraints': indexMapping(
        ['time','job ID','task index','comparison operator','attribute name','attribute value']
    ),
    'taskEvents': indexMapping(
        ['time','missing info','job ID','task index','machine ID',
        'event type','user','scheduling class','priority','CPU request',
        'memory request','disk space request','different machines restriction']
    ),
    'taskUsage': indexMapping(
        ['start time','end time','job ID','task index','machine ID','CPU rate',
        'canonical memory usage','assigned memory usage','unmapped page cache',
        'total page cache','maximum memory usage','disk I/O time','local disk space usage',
        'maximum CPU rate','maximum disk IO time','cycles per instruction','memory accesses per instruction',
        'sample portion','aggregation type','sampled CPU usage']
    )
}

# enumerations
class MachineEventType:
    ADD = 0
    REMOVE = 1
    UPDATE = 2

class JobEventType:
    SUBMIT = 0
    SCHEDULE = 1
    EVICT = 2
    FAIL = 3
    FINISH = 4
    KILL = 5
    LOST = 6
    UPDATE_PENDING = 7
    UPDATE_RUNNING = 8
from pyspark import SparkContext
from schema import *
from statistics import mode, mean

#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[4]")
sc.setLogLevel("ERROR")

def readInputFileAsRDD(filename):
    lines = sc.textFile(filename)
    parts = lines.map(lambda l : l.split(','))
    return parts

# read the input files into RDD[String] structures
jobEvents = readInputFileAsRDD("./data/job_events/part-00000-of-00500.csv")
machineAttributes = readInputFileAsRDD("./data/machine_attributes/part-00000-of-00001.csv")
machineEvents = readInputFileAsRDD("./data/machine_events/part-00000-of-00001.csv")
taskConstraints = readInputFileAsRDD("./data/task_constraints/part-00000-of-00500.csv")
taskEvents = readInputFileAsRDD("./data/task_events/part-00000-of-00500.csv")
taskUsage = readInputFileAsRDD("./data/task_usage/part-00000-of-00500.csv")

def q1():
    print("""
---- Question 1 -----------------------------------------------------------
• What is the distribution of the machines according to their CPU capacity?
""")
    CPUs = columnIndex['machineEvents']['CPUs']
    machineID = columnIndex['machineEvents']['machine ID']

    # filter out null CPUs values
    validEvents = machineEvents.filter(lambda x: bool(x[CPUs]))
    # select machineID and CPU capacity, group by machineID and select minimum capacity
    capacityByMachine = validEvents.map(lambda x: (int(x[machineID]), float(x[CPUs]))).groupByKey().map(lambda x: (x[0], min(x[1])))
    # count capacity occurences
    capacityDistribution = capacityByMachine.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a + b).collect()

    for capacity, count in sorted(capacityDistribution):
        print(f"\t{count} machines with {capacity*100:.0f}% of CPU capacity")


def q2():
    print("""
---- Question 2 -----------------------------------------------------------
• What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
""")


def q3():
    print("""
---- Question 3 -----------------------------------------------------------
• What is the distribution of the number of jobs/tasks per scheduling class? 
""")
    # process infos for jobs
    jobSchedulingClass = columnIndex['jobEvents']['scheduling class']
    jobID = columnIndex['jobEvents']['job ID']

    # select jobsID and scheduling classes, group by joID and select minimum scheduling class
    jobsBySC = jobEvents.map(lambda x: (int(x[jobID]), int(x[jobSchedulingClass]))).groupByKey().map(lambda x: (x[0], min(x[1])))
    # count jobs occurences
    jobsDistribution = jobsBySC.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a + b).collect()

    for sc, count in sorted(jobsDistribution):
        print(f"\t{count} jobs of scheduling class: {sc}")
    
    # process infos for tasks
    print("")
    taskSchedulingClass = columnIndex['taskEvents']['scheduling class']
    taskIndex = columnIndex['taskEvents']['task index']
    
    # select task index and scheduling classes, group by task index and select minimum scheduling class
    tasksBySC = taskEvents.map(lambda x: (int(x[taskIndex]), int(x[taskSchedulingClass]))).groupByKey().map(lambda x: (x[0], min(x[1])))
    # count tasks occurences
    tasksDistribution = tasksBySC.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a + b).collect()
    
    for sc, count in sorted(tasksDistribution):
        print(f"\t{count} tasks of scheduling class: {sc}")


def q4():
    print("""
---- Question 4 -----------------------------------------------------------
• Do tasks with a low scheduling class have a higher probability of being evicted?
""")
    taskSchedulingClass = columnIndex['taskEvents']['scheduling class']
    jobID = columnIndex['taskEvents']['job ID']
    taskIndex = columnIndex['taskEvents']['task index']
    taskEventType = columnIndex['taskEvents']['event type']
    
    # select tuples (job, task) with low scheduling class
    tasksLowSC = set(taskEvents.map(lambda x: (int(x[taskSchedulingClass]),
        (int(x[jobID]), int(x[taskIndex])))).groupByKey().sortBy(lambda x:
                x[0]).take(1)[0][1])
    
    # select tuples (job, task) that were evicted (event type := 2)
    tasksEvicted = set(taskEvents.map(lambda x: (int(x[taskEventType]),
        (int(x[jobID]), int(x[taskIndex])))).filter(lambda x: x[0] ==
                JobEventType.EVICT).map(lambda x: x[1]).collect())

    def intersection(list_a, list_b):
        return [ e for e in list_a if e in list_b ]

    coverage = len(intersection(tasksLowSC, tasksEvicted)) / len(tasksEvicted)
    if coverage > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", {coverage*100:.2f}% of the tasks EVICTed have the lowest Scheduling Class")


def q5():
    print("""
---- Question 5 -----------------------------------------------------------
• In general, do tasks from the same job run on the same machine?
""")
    jobID = columnIndex['taskUsage']['job ID']
    taskIndex = columnIndex['taskUsage']['task index']
    machineID = columnIndex['taskUsage']['machine ID']

    # select tuples (job, set(task, machine)) grouped by jobs
    machinesPerJobs = taskUsage.map(lambda x: (int(x[jobID]),
        (int(x[taskIndex]), int(x[machineID])))).groupByKey().map(lambda x:
                (x[0], set(x[1]))).collect()
    
    # calculate the percentage of the mode (of machineID) over the entire set
    # for each job that scheduled more than one task
    percentagePerJob = []
    for job, s in machinesPerJobs:
        machines = [e[1] for e in s]
        if len(machines) > 1:
            percentagePerJob.append(machines.count(mode(machines)) / len(machines))

    # average usage of the same machine
    meanPerJob = mean(percentagePerJob)
    if meanPerJob > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", usually {round(meanPerJob*100, 2)}% of tasks from the same job run on the same machine")
    print(f"\t(considering only jobs with more than 1 task)")


def q6():
    print("""
---- Question 6 -----------------------------------------------------------
• Are the tasks that request the more resources the one that consume the more resources?
""")
    taskCPURequest = columnIndex['taskEvents']['CPU request']
    eventJobID = columnIndex['taskEvents']['job ID']
    taskEventIndex = columnIndex['taskEvents']['task index']
    taskCPURate = columnIndex['taskUsage']['CPU rate']
    usageJobID = columnIndex['taskUsage']['job ID']
    taskUsageIndex = columnIndex['taskUsage']['task index']
    
    def intersection(list_a, list_b):
        return [ e for e in list_a if e in list_b ]
    
    # select tuples (job, task) that request the more CPU
    tasksRequestMore = taskEvents.map(lambda x: (x[taskCPURequest],
        (int(x[eventJobID]), int(x[taskEventIndex])))).filter(lambda x: x[0] !=
                '').map(lambda x: (float(x[0]), x[1])).sortBy(lambda x: x[0],
                        False).map(lambda x: x[1]).take(100)
    
    # select tuples (job, task) that consume more resources (CPU)
    tasksConsumeMore = taskUsage.map(lambda x: ((int(x[usageJobID]), int(x[taskUsageIndex])),
        float(x[taskCPURate]))).groupByKey().map(lambda x: (x[0],
            mean(x[1]))).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(100)
    
    coverage = len(intersection(tasksRequestMore, tasksConsumeMore)) / len(tasksConsumeMore)
    if coverage > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", {coverage*100:.2f}% of the tasks who consume more CPU have requested it the most")


    taskMemoryRequest = columnIndex['taskEvents']['memory request']
    eventJobID = columnIndex['taskEvents']['job ID']
    taskEventIndex = columnIndex['taskEvents']['task index']
    taskCanonicalMem = columnIndex['taskUsage']['canonical memory usage']
    usageJobID = columnIndex['taskUsage']['job ID']
    taskUsageIndex = columnIndex['taskUsage']['task index']
    
    # select tuples (job, task) that request the more memory
    tasksRequestMore = taskEvents.map(lambda x: (x[taskMemoryRequest],
        (int(x[eventJobID]), int(x[taskEventIndex])))).filter(lambda x: x[0] !=
                '').map(lambda x: (float(x[0]), x[1])).sortBy(lambda x: x[0],
                        False).map(lambda x: x[1]).take(100)
    
    # select tuples (job, task) that consume more resources (Memory)
    tasksConsumeMore = taskUsage.map(lambda x: ((int(x[usageJobID]), int(x[taskUsageIndex])),
        float(x[taskCanonicalMem]))).groupByKey().map(lambda x: (x[0],
            mean(x[1]))).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(100)
    
    coverage = len(intersection(tasksRequestMore, tasksConsumeMore)) / len(tasksConsumeMore)
    if coverage > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", {coverage*100:.2f}% of the tasks who consume more memory have requested it the most")


    taskDiskRequest = columnIndex['taskEvents']['disk space request']
    eventJobID = columnIndex['taskEvents']['job ID']
    taskEventIndex = columnIndex['taskEvents']['task index']
    taskDiskUsage = columnIndex['taskUsage']['local disk space usage']
    usageJobID = columnIndex['taskUsage']['job ID']
    taskUsageIndex = columnIndex['taskUsage']['task index']
    
    # select tuples (job, task) that request the more disk space
    tasksRequestMore = taskEvents.map(lambda x: (x[taskDiskRequest],
        (int(x[eventJobID]), int(x[taskEventIndex])))).filter(lambda x: x[0] !=
                '').map(lambda x: (float(x[0]), x[1])).sortBy(lambda x: x[0],
                        False).map(lambda x: x[1]).take(100)
    
    # select tuples (job, task) that consume more resources (Disk space)
    tasksConsumeMore = taskUsage.map(lambda x: ((int(x[usageJobID]), int(x[taskUsageIndex])),
        float(x[taskDiskUsage]))).groupByKey().map(lambda x: (x[0],
            mean(x[1]))).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(100)
    
    coverage = len(intersection(tasksRequestMore, tasksConsumeMore)) / len(tasksConsumeMore)
    if coverage > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", {coverage*100:.2f}% of the tasks who consume more local disk space have requested it the most")


def q7():
    print("""
---- Question 7 -----------------------------------------------------------
• Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
""")

    
    def intersection(list_a, list_b):
        return [ e for e in list_a if e in list_b ]
    
    time = columnIndex['taskEvents']['time']
    eventType = columnIndex['taskEvents']['event type']
    
    # select moments of EVICTed tasks
    timeEvict = taskEvents.filter(lambda x: int(x[eventType]) ==
            JobEventType.EVICT).map(lambda x: x[time]).sortBy(lambda x:
                    x[0]).collect()


    taskCPURate = columnIndex['taskUsage']['CPU rate']
    usageMachineID = columnIndex['taskUsage']['machine ID']
    taskUsageIndex = columnIndex['taskUsage']['']
    
    def consumedResource(elem):
        return sum([e[1] for e in elem])

    # select tuples (job, task) that consume more resources (CPU)
    timeConsumeMoreCPU = taskUsage.map(lambda x: (int(x[usageMachineID]),
        int(x[taskUsageIndex]), float(x[taskCPURate]))).groupByKey().map()
    #.map(lambda x: (x[0],
     #       mean(x[1]))).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(100)
    
    print(timeConsumeMoreCPU.collect())

    """
    taskCanonicalMem = columnIndex['taskUsage']['canonical memory usage']
    usageJobID = columnIndex['taskUsage']['job ID']
    taskUsageIndex = columnIndex['taskUsage']['task index']
    
    # select tuples (job, task) that consume more resources (Memory)
    timeConsumeMoreMemory = taskUsage.map(lambda x: ((int(x[usageJobID]), int(x[taskUsageIndex])),
        float(x[taskCanonicalMem]))).groupByKey().map(lambda x: (x[0],
            mean(x[1]))).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(100)
    

    taskDiskUsage = columnIndex['taskUsage']['local disk space usage']
    usageJobID = columnIndex['taskUsage']['job ID']
    taskUsageIndex = columnIndex['taskUsage']['task index']
    
    # select tuples (job, task) that consume more resources (disk)
    timeConsumeMoreDisk = taskUsage.map(lambda x: ((int(x[usageJobID]), int(x[taskUsageIndex])),
        float(x[taskDiskUsage]))).groupByKey().map(lambda x: (x[0],
            mean(x[1]))).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(100)
    

    coverage = len(intersection(tasksRequestMore, tasksConsumeMore)) / len(tasksConsumeMore)
    if coverage > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", {coverage*100:.2f}% of the tasks who consume more local disk space have requested it the most")
    """

if __name__ == "__main__":

    q1()
    #q2()
    q3()
    q4()
    q5()
    q6()
    #q7()

    input("Press Enter to continue...")

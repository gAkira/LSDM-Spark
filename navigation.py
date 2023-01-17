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
    capacityDistribuition = capacityByMachine.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a + b).collect()

    for capacity, count in sorted(capacityDistribuition):
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
    jobsDistribuition = jobsBySC.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a + b).collect()

    for sc, count in sorted(jobsDistribuition):
        print(f"\t{count} jobs of scheduling class: {sc}")
    
    # process infos for jobs
    print("")
    taskSchedulingClass = columnIndex['taskEvents']['scheduling class']
    taskIndex = columnIndex['taskEvents']['task index']
    
    # select task index and scheduling classes, group by task index and select minimum scheduling class
    tasksBySC = taskEvents.map(lambda x: (int(x[taskIndex]), int(x[taskSchedulingClass]))).groupByKey().map(lambda x: (x[0], min(x[1])))
    # count tasks occurences
    tasksDistribuition = tasksBySC.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a + b).collect()
    
    for sc, count in sorted(tasksDistribuition):
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
                2).map(lambda x: x[1]).collect())

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
    machineID = columnIndex['taskUsage']['machine ID']

    # list of distinct job IDs (you can use [:10] to get a sample of 10 jobs, for exemple)
    distinctJobs = taskUsage.map(lambda x: x[jobID]).distinct().collect()
    # store percentage of tasks running on the same machine
    tasksSameMachine = []
    # for each job, return list of machines being used for the tasks
    for job in distinctJobs:
        machinesRunningJob = taskUsage.filter(lambda x: x[jobID] == job).map(lambda x: int(x[machineID])).collect()
        # append the percentage of same machines being used (mode) over all the machines
        tasksSameMachine.append(machinesRunningJob.count(mode(machinesRunningJob)) / len(machinesRunningJob))
    
    # average usage of the same machine
    meanPerJob = mean(tasksSameMachine)
    if meanPerJob > 0.5:
        print(f"\tYes", end='')
    else:
        print(f"\tNo", end='')
    print(f", usually {round(meanPerJob*100, 2)}% of tasks run on the same machine")


def q6():
    print("""
---- Question 6 -----------------------------------------------------------
• Are the tasks that request the more resources the one that consume the more resources?
""")
    


def q7():
    print("""
---- Question 7 -----------------------------------------------------------
• Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
""")


if __name__ == "__main__":

    #q1()
    #q2()
    #q3()
    q4()
    #q5()
    #q6()
    #q7()

    input("Press Enter to continue...")

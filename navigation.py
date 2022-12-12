from pyspark import SparkContext
from schema import *

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


# print("""
# ---- Question 2 -----------------------------------------------------------
# • What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
# """)

# print("""
# ---- Question 3 -----------------------------------------------------------
# • What is the distribution of the number of jobs/tasks per scheduling class? 
# """)

# print("""
# ---- Question 4 -----------------------------------------------------------
# • Do tasks with a low scheduling class have a higher probability of being evicted?
# """)

# print("""
# ---- Question 5 -----------------------------------------------------------
# • In general, do tasks from the same job run on the same machine?
# """)

# print("""
# ---- Question 6 -----------------------------------------------------------
# • Are the tasks that request the more resources the one that consume the more resources?
# """)

# print("""
# ---- Question 7 -----------------------------------------------------------
# • Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
# """)

if __name__ == "__main__":

    q1()

    input("Press Enter to continue...")

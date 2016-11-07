#!/usr/bin/env python

import sys
import csv

graph = {}
graph2 = {}

def buildNet(batch_file):
    """ Read from batch_payment.csv to build the graph """
    with open(batch_file, 'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None) # Skip header
        print "Reading batch_payment ..."
        c = 0
        for row in reader:
            c += 1
            if len(row) > 3:
                u1, u2 = row[1], row[2]
                # Build undirected graph using adjacency list
                insertRecord(u1, u2)
                insertRecord(u2, u1)

            if c % 500000 == 0:
                print str(c) + " transactions read"

        print "Finished reading!"
        print "Totally " + str(c) + " transactions read"
        print


def insertRecord(k, v):
    """ Insert k and v into dictionary graph """
    if k in graph:
        if v not in graph[k]:
            graph[k].append(v)
    else:
        graph[k] = []
        graph[k].append(v)


def secondLevel(start):
    """ Use Breadth-first Search to generate a dictionary of
    Second-degree friends list """
    visited = set()
    visited.add(start)
    level2 = set()

    for neighbour in graph[start]:
        if neighbour not in visited:
            visited.add(neighbour)
            for node in graph[neighbour]:
                if node not in visited:
                    level2.add(node)

    graph2[start] = level2


def checkLevel2(start, goal):
    """ Helper method to check whether goal node is within the second level
    of connection with node start """
    # Check whether goal node is in the hash table (first level of connection)
    if goal in graph[start]:
        return True
    # Check whether goal node is in the next level hash table
    elif goal in graph2[start]:
        return True
    else:
        return False


def buildSecond():
    """ Generate second level of the dictionary
    """
    print "Building dictionary"
    for i in graph:
        secondLevel(i)
    print "Done with dictionary"
    print


def detection(stream_file, feat1_file, feat2_file, feat3_file):
    """ Read from stream_payment.csv and write to three text files
    for the results of three features
    """
    f1 = open(feat1_file,'w')
    f2 = open(feat2_file,'w')
    f3 = open(feat3_file,'w')

    with open(stream_file, 'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        # Skip header
        next(reader, None)
        print "Reading stream_payment for detection..."
        c = 0
        for row in reader:
            c += 1
            if c % 500000 == 0:
                print str(c) + " transactions processed"
            if len(row) > 3:
                u1, u2 = row[1], row[2]

                # u1 not in the graph
                if u1 not in graph:
                    f1.write("unverified\n")
                    f2.write("unverified\n")
                    f3.write("unverified\n")
                # u1 in graph
                else:
                    # First level friend (direct payment before)
                    if u2 in graph[u1]:
                        f1.write("trusted\n")
                        f2.write("trusted\n")
                        f3.write("trusted\n")
                    # u2 not in graph (not first level)
                    # Feature 1 failed
                    # Check feature 2 and 3
                    else:
                        f1.write("unverified\n")
                        # 2nd-degree friend?
                        if checkLevel2(u1, u2):
                            f2.write("trusted\n")
                            f3.write("trusted\n")
                        # Feature 2 failed
                        # Check feature 3
                        else:
                            f2.write("unverified\n")
                            for value in graph2[u1]:
                                # 2nd-degree friend of 2nd-degree freiend?
                                if checkLevel2(value, u2):
                                    f3.write("trusted\n")
                                    break
                                # Out of 4th-degree
                                # Feature 3 failed
                                else:
                                    f3.write("unverified\n")
                                    break

        print "Finished detections!"
        print "Totally " + str(c) + " transactions"
        print "Please check the output directory for results..."

    f1.close()
    f2.close()
    f3.close()


def main():
    buildNet(sys.argv[1])
    buildSecond()
    detection(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])


if __name__ == "__main__":
    import time
    start_time = time.time()
    main()
    print "--- %s seconds ---" % (time.time() - start_time)

#!/usr/bin/env python

import sys
import csv

# print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)

# for i in range(len(sys.argv)):
#     print sys.argv[i]

# with open(sys.argv[1], 'rU') as csvfile:
#     reader = csv.reader(csvfile, delimiter=',')
#     for row in reader:
#         print row

graph = {}

graph2 = {}

def buildNet(batch_file):
    """ Read from batch_payment.csv to build the network """
    with open(batch_file, 'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)
        print
        print "Reading records..."
        c = 0
        for row in reader:
            c += 1
            #line = row.split(',')
            if len(row) > 3:
                u1, u2 = row[1], row[2]
                # print row, u1, u2
                insertRecord(u1, u2)
                insertRecord(u2, u1)

            if c % 500000 == 0:
                print str(c) + " read"

        print
        print "Finished reading!"
        print "Totally " + str(c) + " records"
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
    """ Use Breadth-first Search to generate a dictionary of Second-degree friends list """
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
    """ Helper method to check whether node goal is within the second level
    of connection with node start """
    # Check whether goal is in the hash table (first level of connection)
    if goal in graph[start]:
        return True
    elif goal in graph2[start]: # Check whether is in the second level hash table
        return True
    else:
        return False

def buildSecond():
    print "Building dictionary"
    for i in graph:
        secondLevel(i)
    print "Done with dictionary"
    print

def detection(stream_file, feat1_file, feat2_file, feat3_file):
    f1 = open(feat1_file,'w')
    f2 = open(feat2_file,'w')
    f3 = open(feat3_file,'w')

    with open(stream_file, 'rU') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        # Skip header
        next(reader, None)
        print "Reading transactions for detection..."
        c = 0
        for row in reader:
            c += 1
            if c % 500000 == 0:
                print str(c) + " transactions processed"
            if len(row) > 3:
                u1, u2 = row[1], row[2]
                # print u1, u2
                if u1 not in graph:
                    f1.write("unverified\n")
                    f2.write("unverified\n")
                    f3.write("unverified\n")
                else: # u1 in graph
                    if u2 in graph[u1]:
                        f1.write("trusted\n")
                        f2.write("trusted\n")
                        f3.write("trusted\n")
                    else: # u2 not in graph
                        f1.write("unverified\n")
                        if checkLevel2(u1, u2):
                            f2.write("trusted\n")
                            f3.write("trusted\n")
                        else:
                            f2.write("unverified\n")
                            for value in graph2[u1]:
                                if checkLevel2(value, u2):
                                    f3.write("trusted\n")
                                    break
                                else:
                                    f3.write("unverified\n")
                                    break

        print "Finished detection!"
        print "Totally " + str(c) + " transactions"

    f1.close()
    f2.close()
    f3.close()

def main():
    buildNet(sys.argv[1])
    buildSecond()
    detection(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

if __name__ == "__main__":
   main()

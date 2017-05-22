# -*- coding: utf-8 -*-
"""
Created on Sun May 21 19:57:14 2017

@author: starr
"""
import os

iterations = 5

fileToRun = raw_input("Enter input file name: ")

clusters = raw_input("Enter number of clusters: ")

centroidsCMD = "shuf -n "+ clusters + " "+fileToRun+" > centroids.txt"
centroidstoHDFSCMD = "hdfs dfs -copyFromLocal -f centroids.txt datahw3/input/centroids.txt"
runCMD = "yarn jar mr-app-1.0-SNAPSHOT.jar com.msia.app.datahw3.Kmeans datahw3/"+fileToRun+" datahw3/output"
collectCMD = "hdfs dfs -getmerge datahw3/output centroids.txt"
rmCRCCMD = "rm .centroids.txt.crc"
saveInputCMD = "hdfs dfs -copyFromLocal -f centroids.txt datahw3/input/centroids.txt"

os.system(rmCRCCMD)
os.system(centroidsCMD)
os.system(centroidstoHDFSCMD)

counter = 0
while (counter < iterations):
    os.system(runCMD)
    os.system(collectCMD)
    os.system(rmCRCCMD)
    os.system(saveInputCMD)
    counter += 1

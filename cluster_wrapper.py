#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 19 14:19:55 2017

@author: Julia Greenberger

Python wrapper code to run k-means iteratively using MapReduce
"""

import os 
import sys

def main(nb_iter = sys.argv[1], nb_clust = sys.argv[2], filename = sys.argv[3], outdir = sys.argv[4]):
    
    '''
    ==========================
    Define os commands 
    ==========================
    '''
    
    #clean directory
    rm_dir = 'hdfs dfs -rm -r hw3/output/' + outdir
    
    #remove previous centroid files
    rm_cent_hdfs = 'hdfs dfs -rm hw3/input/centroids.txt'
    rm_cent_local = 'rm centroids.txt'
    rm_crc = 'rm .centroids.txt.crc'
    
    #sample random rows as initial centroids
    init_cent = 'hdfs dfs -cat hw3/input/' + filename + ' | shuf -n ' + nb_clust + ' > centroids.txt'
    
    #copy centroids.txt to hadoop from local wolf
    copyFromLocal = 'hdfs dfs -put centroids.txt hw3/input/'
        
    #run mapreduce
    runMR = 'hadoop jar mr-app-1.0-SNAPSHOT.jar com.javamakeuse.hadoop.poc.Homework3.Kmeans hw3/input/' + filename + ' hw3/output/' + outdir
    
    #save getmerge output to local wolf
    getmerge = 'hdfs dfs -getmerge hw3/output/' + outdir + ' centroids.txt'
    
    
    '''
    ==========================
    Initialize centroids 
    ==========================
    '''
    #Clear directories and files
    os.system(rm_dir)
    os.system(rm_cent_hdfs)
    os.system(rm_cent_local)
    os.system(rm_crc)

    #Initialize centroids
    os.system(init_cent)
    os.system(copyFromLocal)

    for i in range(0,int(nb_iter)):
        
        '''
        ==========================
        Iterate k-means  
        ==========================
        '''
        print('Running MapReduce...')
        os.system(runMR)
        os.system(getmerge)
        os.system(rm_cent_hdfs)
        os.system(copyFromLocal)
        os.system(rm_dir)
        print('Iteration complete.')

if __name__ == '__main__':
    main()


#!/usr/bin/env python3

#Optimized for lower memory usage
#Usage:
#./NCD_optimized.py <out_base_name> <infile1> <infile2> <infile3>
#e.g.
#./NCD_optimized.py ncd_matrix sample_policies/sample/*

import sys
import math
import os
import lzma
import io
import csv
from multiprocessing import Pool
import multiprocessing
import subprocess

compressed = {}

def progress(s):
  sys.stderr.write(s)
  sys.stderr.write('\n')

def Z(contents, filters):
    return len(lzma.compress(contents, format=lzma.FORMAT_RAW, filters=filters))

    
def ncd_pure(idA, idB, ca,cb, filters):
    if idA not in compressed:
        compressed[idA] = Z(ca,filters)
    if idB not in compressed:
        compressed[idB] = Z(cb,filters)
    Za = compressed[idA]
    Zb = compressed[idB]
    Zab = Z(ca + cb, filters)
    return (Zab - min(Za, Zb)) / max(Za, Zb)

  
def ncd(fa,fb, filters):
    #if fa not in compressed:
    #    compressed[fa] = Z(fa,filters)
    #if fb not in compressed:
    #    compressed[fb] = Z(fb,filters)
    Za = compressed[fa]
    Zb = compressed[fb]
    Zab = Z(contents[fa] + contents[fb], filters)
    return (Zab - min(Za, Zb)) / max(Za, Zb)

def calculate_row(f,filters):
    elements = 0
    data = {"file" : f}
    for f2 in files:
        if f >= f2:
            data[f2] = ncd(f, f2,filters)
            elements += 1
        else:
            data[f2] = ''
    return data, elements

def add_result(tup):
    rowCt, elemCt = tup
    global totalRows
    global totalElems
    totalRows += rowCt
    totalElems += elemCt
#    progress("progress: %f %%" % ((float(totalRows) / len(files)) * 100))
    progress("progress: %f %%" % ((float(totalElems) / allElems) * 100))
    
def calculate_region(fs,idnum,filters):
    region_name="%s_%d" % (sys.argv[1], idnum)
    element_ct = 0
    with open("%s.csv" % region_name, "w", newline='') as csvfile:
        output =  csv.DictWriter(csvfile, ["file"] + files)
        for f in fs:
            row, element_ct_r = calculate_row(f,filters)
            element_ct += elemnt_ct_r
            output.writerow(row)
    return (len(fs), element_ct)


if __name__ == "__main__":

  files = sorted([f for f in sys.argv[2:] if os.path.getsize(f) > 0])
  sizes = [os.path.getsize(f) for f in files]

  progress("Reading all files")
  contents = { f : io.FileIO(f).readall() for f in files}


  lzma_filters = my_filters = [ #LZMA is a good choice since it does not break into blocks
      {
        "id": lzma.FILTER_LZMA2, 
        "preset": 9 | lzma.PRESET_EXTREME, 
        "dict_size": max(sizes) * 10, # a big enough dictionary, but not more than needed, saves memory
        "lc": 3,
        "lp": 0,
        "pb": 0, # assume ascii
        "mode": lzma.MODE_NORMAL,
        "nice_len": 273,
        "mf": lzma.MF_BT4
      }
  ]

  progress("Compressing all files")
  compressed = { f : Z(contents[f], lzma_filters) for f in files }

  if len(compressed) == 1:
      print(compressed)
      sys.exit()



  if len(compressed) == 2:
      print(ncd(files[0], files[1],lzma_filters))
      sys.exit()

  progress("Calculating ncd's")

  data = {}

  totalRows = 0
  totalElems = 0
  allElems = len(files) * len(files) / 2



  region_size = 100
  regions = []
  region_ct = int(math.ceil(len(files)/region_size))

  for i in range(region_ct):
      start = i * region_size
      end = min(len(files), (i+1) * region_size)
      regions.append(files[start:end])

  with Pool(multiprocessing.cpu_count()) as p:
      for i in range(0,region_ct):
          p.apply_async(calculate_region, args = (regions[i],i,filters,), callback=add_result) 
      p.close()
      p.join()

  with open("%s.csv" % sys.argv[1], "w") as csvfile:
      output =  csv.DictWriter(csvfile, ["file"] + files)
      output.writeheader()
  with open("%s.csv" % sys.argv[1], "a") as csvfile:
      subprocess.call(["cat"] + ["%s_%d.csv" % (sys.argv[1], idnum) for idnum in range(region_ct)], stdout=csvfile)

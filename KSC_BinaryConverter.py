# Program to convert electric field mill data
# from binary KSC to ASCII csv averaged
# every minute
#
# For other applications, this should be easy enough
# to modify to the users desire.
#
# Written in python3, should be easy
# to convert to python2 if required
#
# Run it with the following command:
#   python3 KSC_Binary_Converter.py
#
# Author: Greg Lucas
# e-mail: greg.m.lucas@gmail.com
#
# To convert to yearly data here is a bash line
# take the header from a single file as they are
# all the same. Then tail from the second line
# down all files with the right year and append
# that to the year.csv file
# Need to declare year=1997 or similar first
#
# head -1 1997_226.csv > yearly/${year}.csv;
# tail -n +2 -q ${year}_*.csv >> yearly/${year}.csv
# 
######################################################               

import struct
import csv
import datetime

import os
import sys
import zipfile
import tarfile
import glob
import urllib.request

##########################################################
# Download data or not?
##########################################################
#
# So people don't go downloading gigabytes of data
# without thinking about it first, this will be
# set to False at first. Change it to True to
# run the routines for the year/days requested.
download_data = False
# Dates you are interested in processing
years = range(1997, 2013)
days = range(1, 366)

##########################################################

# For displaying progress of downloads
def reporthook(blocknum, blocksize, totalsize):
    readsofar = blocknum * blocksize
    if totalsize > 0:
        percent = readsofar * 1e2 / totalsize
        s = "\r%5.1f%% %*d / %d" % (
            percent, len(str(totalsize)), readsofar, totalsize)
        sys.stderr.write(s)
        if readsofar >= totalsize: # near the end
            sys.stderr.write("\n")
    else: # total size is unknown
        sys.stderr.write("read %d\n" % (readsofar,))


# This is the field mill structure
""" Field Structure

TRMM Raw Data Archives

Directory naming convention: [FM.YYYY.MM.DD]

File naming convention:  YYYYMMDDhhmm.DAT

   YYYY = 4 digit year (1960-2059)
   MM = 2 digit month (00-12)
   DD = 2 digit day (00-31)
   hh = 2 digit hour (00-23)
   mm = 2 digit minute (00,30)

All times are represented in GMT format.

File attributes: 7184 byte fixed-length binary records.

File contents: 30 minutes of field mill data

Record structure:

   Byte    Byte
   Offset  Count    Description
   ------  -----    ------------------------------
      1       4     Reserved (0x89,0x89,0x89,0x89)
      5       1     Year (last 2 digits)
      6       1     Month (1-12)
      7       1     Day (1-31)
      8       1     Hour (0-23)
      9       1     Minutes (0-59)
     10       1     Seconds (0-59)
     11       1     Hundredths of Seconds (unused)
     12       1     Milliseconds (unused)
     13       1     Number of mills
     14       3     Spare
     17    7168     Mill data

   Mill data: 64 mills (112 bytes each)

   Byte    Byte
   Offset  Count    Description
   ------  -----    ------------------------------
      1       1     Address
      2       1     Mode
      3       4     Status
      7       1     Playback
      8       2     Reserved
     10       1     Rain Gauge
     11     100     50 Hz Data (16-bit values)
    111       2     CRC (16-bit value)
      
Note: For more detailed information on field values, see file
      FM_STRUCTURE.DOC.
      
"""

"""
    I = Two's complement 16 bit integer, Motorola byte order
	UI = Unsigned 16 bit integer
	BI = 16 bit binary pattern
	C = Two's complement 8 bit integer
	UC	=	Unsigned 8 bit integer
	BC = 8 bit binary bit pattern
	X = Not defined

	Byte no.	Type	Function
	1-2				BI		Synchronization Pattern
	3					UC		Station Address
	4					BC		Mode/Command Byte
	5-11			-			Status Bytes 1-7
	12				UC		Rain Gauge Tip Count
	13-112		I/-		Potential Gradient Data or Extended Status Report
	113-114		BI		CRC-16 Value
"""

# This is the actual binary conversion to csv
# It takes the file name and a given writer that
# is already opened to write the data output to
def translate_KSC(infile, writer):

    chunksize = 7184
    chunk2size = 112
    
    second_count = 0
    
    with open(infile, 'rb') as f:
        while True:
            chunk = f.read(chunksize)
            if second_count == 0:
                # Storage for 1 minute averages
                mill_counts = [0. for i in range(34)] #np.zeros(34)
                mill_values = [0. for i in range(34)] #np.zeros(34)
                
            if chunk:
                year, month, day, hour, minute, second = struct.unpack('6B', chunk[4:10])
                # Year represented as 96, or 10 for 1996 and 2010
                if year < 90:
                    year = year + 2000
                else:
                    year = year + 1900
                time = "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}".format(year, month, day, hour, minute)
                    
                chunk2 = chunk[16:]
                # For every mill (31 of them, extending out to station 34) unpack the data
                for i in range(34):
                    # Address, mode, 7x status + extra, rain
                    x = struct.unpack("BB7xB50h2x", chunk2[i*chunk2size: (i+1)*chunk2size])
                    # If the mode for this station is bad continue on past it
                    if x[1] != 1: continue
                        
                    address = x[0]
                    rain = x[2]
                    # Removed the dependence on np.mean as then people don't
                    # need to have numpy installed.
                    mill_values[address-1] += float(sum(x[3:]))/len(x[3:])
                    mill_counts[address-1] += 1
            else:
                break
            
            second_count += 1
            # only write the output every minute
            if second_count == 60:
                vals = [mill_values[i]/mill_counts[i] if mill_counts[i] > 0 else float('nan')
                        for i in range(len(mill_values))]

                writer.writerow([time] + vals)
                second_count = 0

#########################
# Downloading routines
#########################

# Download the file from `url` and save it locally under `file_name`:

tardir = 'tars/'
tempdir = 'temp/'
storagedir = 'KSC_data/'

if not os.path.exists(storagedir):
    os.makedirs(storagedir)
    
if not os.path.exists(tardir):
    os.makedirs(tardir)
    
if not os.path.exists(tempdir):
    os.makedirs(tempdir)
    
def untar_file(name):
    tf = tarfile.open(tf_name)
    tf.extractall(tempdir)
    
def unzip_files():
    # Now we have a bunch of zips in the tempdir
    files = sorted(glob.glob(tempdir + "*.zip"))
    for f in files:
        zf = zipfile.ZipFile(f)
        zf.extractall(tempdir)
        os.remove(f)

    # Clean up the other garbage files/images
    other_files = glob.glob(tempdir + "*")
    for f in other_files:
        os.remove(f)
        
def process_files(outfile):
    # Now we have a bunch of .dat files
    files = sorted(glob.glob(tempdir + "*.dat"))
    with open(outfile, 'w') as csvfile:
        writer=csv.writer(csvfile)
        # Header
        writer.writerow(["Time"] + [i+1 for i in range(34)])
        for f in files:
            # Not all files are correct size, so just ignore those
            # as nothing can be said about the binary format if it
            # isn't the correct size
            if os.path.getsize(f) == 12931200:
                translate_KSC(f, writer)
            else:
                print("Error in file:", f)
            os.remove(f)
    print("Processing Complete:", outfile)

# This is the URL for the global hydrology resource center ftp site
# Data on this site goes from 1997-2012
# Note that this won't download more data than necessary, but if it
# does encounter an error, there may be garbage left in the temp
# directory

# Should we download the data or not?
if not download_data:
    print("download_data = False")
    print("Change this flag to process the data")
    os.sys.exit()

base_url = "ftp://ghrc.nsstc.nasa.gov/pub/data/ksc-fieldmill/"
for year in years:
    for day in days:
        file_name = "agbfm_{0:04d}.{1:03d}_daily.tar".format(year, day)
        url = base_url + "{0:04d}/".format(year) + file_name
        tf_name = tardir + file_name
        
        outfile = storagedir + "{0:04d}_{1:03d}.csv".format(year, day)

        # We've already processed the data, so don't worry about this year/day
        if os.path.exists(outfile):
            print("Already processed:", year, day)
            continue
        
        if os.path.exists(tf_name):
            print("Already downloaded:", year, day)
            try:
                untar_file(tf_name)
            
                unzip_files()
    
                process_files(outfile)
            except:
                print("Data Error:", year, day)
        else:
            try:
                #urllib.request.urlretrieve(url, tf_name, reporthook)
                urllib.request.urlretrieve(url, tf_name)
            except:
                print("Error: No file", url)
                continue

            try:
                untar_file(tf_name)
                
                unzip_files()
        
                process_files(outfile)

                os.remove(tf_name)
            except:
                print("Data Error:", year, day)


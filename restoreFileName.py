#!/usr/bin/env python
'''
    Author: Marco Boretto
    Mail: marco.boretto@cern.ch
    Python Version: 2.7
    This script allows you to rename file like 'myfile.dat.del' in myfile.dat
'''
import os

def main():
    source_dir = '/merger/cdr'
    for new_file in os.listdir(source_dir):
        file_path = source_dir + '/' + new_file
        filename, file_extension = os.path.splitext(file_path)
        if file_extension == '.del':
            new_path = filename
            print 'Deleted File found Renaming: ' + file_path + ' in ' + new_path
            os.rename(file_path, new_path)

if __name__ == '__main__':
    main()

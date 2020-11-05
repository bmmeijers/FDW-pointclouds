import csv
import glob
import pandas
import time
import csv
import subprocess
from laspy.file import File
import numpy as np
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from multicorn.utils import ERROR, DEBUG, WARNING, INFO

class SystemFdw(ForeignDataWrapper):
    """ 
    a foreign data wrapper for different point clouds files on the same file system
    valid options:
        filepath : full path to this file system
    """
    #def hello(file):
     #       log_to_postgres('hello')
    
    def __init__(self, fdw_options, fdw_columns):
        super(SystemFdw, self).__init__(fdw_options, fdw_columns)
        log_to_postgres('options: %s' % fdw_options, DEBUG)
        log_to_postgres('columns: %s' % fdw_columns, DEBUG)
        
        if 'filepath' in  fdw_options:
            self.filepath = fdw_options["filepath"]
        else:
            log_to_postgres('filepath parameter is required.', ERROR)      
    
    def execute(self, quals, columns, sortkeys = None):
        #log_to_postgres('execute  {} {} {}'.format(quals, columns, sortkeys), INFO)
        
        if quals:
            log_to_postgres('Get the box range')
            for qual in quals:
                #log_to_postgres(qual)
                if qual.field_name == 'x' and (qual.operator == '>' or qual.operator == '>='):
                    xmin = qual.value
                    log_to_postgres('box left range is {}'.format(xmin))
                if qual.field_name == 'x' and (qual.operator == '<' or qual.operator == '<='):
                    xmax = qual.value
                    log_to_postgres('box right range is {}'.format(xmax))
                if qual.field_name == 'y' and (qual.operator == '>' or qual.operator == '>='):
                    ymin = qual.value
                    log_to_postgres('box down range is {}'.format(ymin))
                if qual.field_name == 'y' and (qual.operator == '<' or qual.operator == '<='):
                    ymax = qual.value
                    log_to_postgres('box up range is {}'.format(ymax))
        
        files_info = {} #dict
        filepath = self.filepath
        metafile = '{}/{}'.format(filepath, 'metadata.csv')
        
        with open(metafile, newline = '') as csvfile: 
            reader = csv.DictReader(csvfile)
            for row in reader:
                min_x = float(row['min_x'])
                max_x = float(row['max_x'])
                min_y = float(row['min_y'])
                max_y = float(row['max_y'])
                if quals:
                    # condition of whether overlapping
                    x_bool = (xmin > min_x and xmin < max_x) or (xmax > min_x and xmax < max_x) or (xmin < min_x and xmax > max_x)
                    y_bool = (ymin > min_y and ymin < max_y) or (ymax > min_y and ymax < max_y) or (ymin < min_y and ymax > max_y)
                    # overlapping
                    if x_bool and y_bool:
                        files_info[row['filename']] = [row['format'],
                                                       min_x,
                                                       max_x,
                                                       min_y,
                                                       max_y,]
                else:
                   files_info[row['filename']] = [row['format'],
                                                       min_x,
                                                       max_x,
                                                       min_y,
                                                       max_y,]
            if len(files_info) > 1:
                log_to_postgres('There are {} relevant files'.format(len(files_info)))
            if len(files_info) == 1:
                log_to_postgres('There is 1 relevant file')
            if len(files_info) == 0:
                log_to_postgres('There is no relevant file')
        
        def execute_lastools(cmd):
            #log_to_postgres('execute lastools command')
            popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, universal_newlines=True)
            for line in iter(popen.stdout.readline, ""):
                # attribute = 'xyzitcupRGB'
                line = line.strip().split()
                record = {
                        'x': float(line[0]),
                        'y': float(line[1]),
                        'z': float(line[2]),
                        'red': float(line[8]),
                        'green': float(line[9]),
                        'blue': float(line[10]),
                        'intensity': float(line[3]),
                        'gps time': float(line[4]),
                        'classification': int(line[5]),
                        'point source id': float(line[6]),
                        'user data': float(line[7]),
                        # to create point columns as string, them to cast to geometry point in the postgis side
                        'point2d': '({}, {})'.format(float(line[0]),float(line[1])),
                        'point3d': '({}, {}, {})'.format(float(line[0]),float(line[1]),float(line[2]))
                        }
                #log_to_postgres(record)
                yield record
                
                # convert ascii line to python floats
                #yield tuple(map(float, line.strip().split(" ")))    
            popen.stdout.close()
            return_code = popen.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, cmd)      
        
        def reader_lastools(file):
            log_to_postgres('read pc using lastools')
            attribute = 'xyzitcupRGB' 
            """
            x, y, z,
            a - scan angle,
            i - intensity,
            n - number of returns for given pulse,
            r - number of this return,
            c - classification, 
            u - user data,
            p - point source ID, 
            e - edge of flight line flagw
            d - direction of scan flag
            red, green, blue
            """
            cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -parse {} -stdout'.format(file, attribute)
            #cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -stdout'.format(file)
            log_to_postgres(cmd)
            for line in execute_lastools(cmd):
                yield line
                pass
               
        def reader_laz(laz_file):
            log_to_postgres('hello')
            reader_lastools(laz_file)
        
        def reader_las(las_file):
            reader_lastools(las_file)
        
        
        start = time.time()
        for filename in files_info.keys():
            #log_to_postgres(filename)
            #log_to_postgres(files_info[filename])
            file = '{}/{}'.format(filepath, filename)                                                                                                                          
            log_to_postgres(file)
            if files_info[filename][0] == 'las':
                #log_to_postgres('to call LAS reader function')
                #log_to_postgres('to call LAZ reader function')
                #reader_lastools(file)
                #reader_laz(file)
                attribute = 'xyzitcupRGB'
                if quals:
                    cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -parse {} -inside {} {} {} {} -stdout'.format(file, attribute,
                                                                                                                       xmin, ymin, xmax, ymax)
                else:
                    cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -parse {} -stdout'.format(file, attribute)
                #cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -stdout'.format(file) 
                #log_to_postgres(cmd)
                for line in execute_lastools(cmd):
                    yield line
                    pass   
                
            if files_info[filename][0] == 'laz': 
                #log_to_postgres('to call LAZ reader function')
                #reader_lastools(file)
                #reader_laz(file)
                attribute = 'xyzitcupRGB'
                if quals:
                    cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -parse {} -inside {} {} {} {} -stdout'.format(file, attribute,
                                                                                                                       xmin, ymin, xmax, ymax)
                else:
                    cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -parse {} -stdout'.format(file, attribute)
                #cmd = '/home/mutian/fdw/LAStools.o/bin/las2txt -i {} -stdout'.format(file) 
                #log_to_postgres(cmd)
                for line in execute_lastools(cmd):
                    yield line
                    pass   
            
            if files_info[filename][0] == 'txt':
                #log_to_postgres('to call TXT reader function')
                with open (file) as stream:
                    reader = stream.readlines()
                    for line in reader:
                        line = line.split() 
                        x = line[0]
                        y = line[1]
                        z = line[2]
                        point = '({}, {})'.format(x, y)
                        record = {
                            'x': x,
                            'y': y,
                            'z': z,
                            'point2d': point
                            }
                        #yield line[:len(self.columns)]
                        yield record
                                     
                        
        end = time.time()
        log_to_postgres('time consuming: {}'.format(end - start))
#!/usr/bin/python
# -*- coding: utf-8 -*-
'''ImportFiles.py

Import data files to ARES

Usage: ImportFiles.py [-h] -d DIR [-t TYPE] [-r RUNTIME]

optional arguments:
  -h, --help                     show this help message and exit
  -d DIR, --dir DIR              Data files directory
  -t TYPE, --type TYPE           Files data type
  -r RUNTIME, --runtime RUNTIME  ARES Runtime Folder

where:

  DIR         Directory to get the data files from
  TYPE       Assumed data type for all the files
  RUNTIME    Directory where the ARES runtime environment is installed
             If non present, then ~/ARES_RUNTIME is assumed, unless
             the env. var. ARES_RUNTIME is set.

Alternatively, you can activate the execution permissions of this script, and
call it directly.

Usage example:

  $ python src/ImportFiles.py --dir $(pwd)/in --runtime $(pwd)/runtime

'''

from shutil import copy

import os, sys
import logging
import argparse
import re
import time
import glob
import json

VERSION = '0.0.1'

__author__ = "jcgonzalez"
__version__ = VERSION
__email__ = "jcgonzalez@sciops.esa.int"
__status__ = "Prototype" # Prototype | Development | Production


# Change INFO for DEBUG to get debug messages
log_level = logging.INFO

# Set up logging information
format_string = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=log_level, format=format_string, stream=sys.stderr)


#----------------------------------------------------------------------------
# Class: Ingestor
#----------------------------------------------------------------------------
class Ingestor(object):
    '''
    Processor is the base class for all the processors to be executed from
    the Euclid QLA Processing Framework, independent if they are run inside or
    outside Docker containers.
    '''

    # Framework environment related variables
    Home = os.environ['HOME']
    AresRuntimeDir = ''

    if os.path.isdir(Home + '/ARES_RUNTIME'):
        AresRuntimeDir = Home + '/ARES_RUNTIME'

    if "ARES_RUNTIME" in os.environ:
        # Nominally, the QPFWA env. variable should point to the QPF Working Area
        # main directory (usually /home/eucops/qpf)
        AresRuntimeDir = os.environ["ARES_RUNTIME"]

    # The following hash table shows a series of regular expresions that can be used
    # to deduce the imported data file type
    AresFileTypes = {}
    AresFileTypesCfgFile = "import_file_types.json"

    def __init__(self, data_dir, ares_runtime=None, data_type=None):
        '''
        Instance initialization method
        '''

        # Get arguments, and incorporate to object
        self.data_dir = data_dir
        self.ares_runtime = Ingestor.AresRuntimeDir
        self.data_type = data_type

        try:
            logging.info('Reading import script config. file'.format(self.ares_runtime))
            this_script_dir = os.path.dirname(os.path.realpath(__file__))
            cfg_file = this_script_dir + '/' + Ingestor.AresFileTypesCfgFile
            self.ares_data_types = json.load(open(cfg_file))
            self.compile_patterns()
        except:
            logging.fatal('Import script config. file not found in {}'
                          .format(cfg_file))

        logging.info('-'*60)

        if ares_runtime:
            self.ares_runtime = ares_runtime

        if not os.path.isdir(self.ares_runtime):
            logging.fatal('ARES system runtime folder {} does not exist'.format(self.ares_runtime))
            os._exit(1)

        logging.info('ARES system runtime folder is {}'.format(self.ares_runtime))
        self.ares_import = self.ares_runtime + '/import'
        logging.info('ARES import folder is {}'.format(self.ares_import))

        self.admin_server_log = self.ares_runtime + '/AdminServer/ares_server.log'
        logging.info('Monitoring ARES Server log file {}'.format(self.admin_server_log))

        # Evaluate input data files
        if not os.path.isdir(self.data_dir):
            logging.fatal('Specified input data folder {} does not exist'.format(self.data_dir))
            os._exit(1)

        self.input_files = glob.glob(self.data_dir + '/*.dat')
        logging.debug(self.input_files)
        if len(self.input_files) < 1:
            logging.fatal('No data files found for ingestion')
            os._exit(1)

    def compile_patterns(self):
        '''
        Compile patterns used to define file data type
        '''
        for typ, data in self.ares_data_types.items():
            self.ares_data_types[typ]['re'] = re.compile(data['re'])

    def tail(self, f, lines=1, _buffer=4098):
        '''
        Tail a file and get X lines from the end
        :param f: File handler
        :param lines: number of lines to take from the end
        :param _buffer: Buffer size
        :return: lines from the end of the file
        '''
        # place holder for the lines found
        lines_found = []

        # block counter will be multiplied by buffer to get the block size from the end
        block_counter = -1

        # loop until we find X lines
        while len(lines_found) < lines:
            try:
                f.seek(block_counter * _buffer, os.SEEK_END)
            except IOError:  # either file is too small, or too many lines requested
                f.seek(0)
                lines_found = f.readlines()
                break

            lines_found = f.readlines()

            # decrement the block counter to get the next X bytes
            block_counter -= 1

        return lines_found[-lines:]

    def wait_until_import_is_completed(self):
        '''
        Continuously monitor ARES server log file to determine whether the import
        was successful
        :return:
        '''
        result = False
        end_monitor = False
        with open(self.admin_server_log, 'r') as f:
            while not end_monitor:
                # Time delay (give some time to do the import)
                time.sleep(1)

                # Check last line
                last_line = self.tail(f, lines = 1)

                # if has been imported
                result = True
                end_monitor = True

                # if has not been imported due to a problem
                # result = False
                # end_monitor = True

                # Otherwise, keep monitoring

        return result

    def run_import(self):
        '''
        Loop over input files, to import each of them sequentally
        '''

        self.compile_patterns()

        logging.info('Import process starting')
        logging.info('-'*60)

        # Main loop on files
        num_of_files = len(self.input_files)
        num_of_imported_files = 0
        num_of_failed_files = 0
        for i, fname in enumerate(self.input_files):
            logging.info('Preparing import of file {} of {}: {}'
                         .format(i + 1, num_of_files, fname))

            # Detect data type
            ftype = None
            fimport_dir = ''
            if self.data_type:
                ftype = self.data_type
                fimport_dir = Ingestor.AresImportFolders[ftype]
            else:
                for typ, typ_info in self.ares_data_types.items():
                    rx = typ_info['re']
                    if rx.match(os.path.basename(fname)):
                        ftype = typ
                        fimport_dir = typ_info['dir']
                        break

            if not ftype:
                logging.warn('Unidentified data file type. Failed import.')
                num_of_failed_files += 1
                continue

            import_dir = self.ares_import + "/" + fimport_dir
            logging.info('Data type: {} (folder: {})'.format(ftype,import_dir))

            # Copy data file to import folder
            copy(fname, import_dir)

            # Wait for the result
            import_result = self.wait_until_import_is_completed()

            if import_result:
                num_of_imported_files += 1
                logging.info('Data file imported successfully')
            else:
                num_of_failed_files += 1
                logging.warn('Data file importing failed!')

        logging.info('-'*60)
        logging.info('Import process completed.')
        logging.info('{} of {} files successfully imported.'
                     .format(num_of_imported_files, num_of_files))
        if num_of_failed_files > 0:
            logging.warn('{} of {} files import failed.'
                         .format(num_of_failed_files, num_of_files))
        logging.info('-'*60)
        logging.info('Done.')

def get_args():
    '''
    Function for parsing command line arguments
    '''
    parser = argparse.ArgumentParser(description='Import data files to ARES',
                                     formatter_class=lambda prog:
                                     argparse.HelpFormatter(prog,
                                                            max_help_position=70))
    parser.add_argument('-d', '--dir',
                        help='Data files directory',
                        dest='dir', required=True)
    parser.add_argument('-t', '--type',
                        help='Files data type',
                        dest='type', default=None, required=False)
    parser.add_argument('-r', '--runtime',
                        help='ARES Runtime Folder',
                        dest='runtime', required=False)

    return parser.parse_args()

def greetings():
    '''
    Says hello
    '''
    logging.info('='*60)
    logging.info('ImportFiles.py - Data Files Tools for ARES')

def main():
    '''
    Main processor program
    '''
    greetings()
    args = get_args()
    ingestor = Ingestor(data_dir=args.dir,
                        ares_runtime=args.runtime,
                        data_type=args.type)
    ingestor.run_import()


if __name__ == "__main__":
    main()

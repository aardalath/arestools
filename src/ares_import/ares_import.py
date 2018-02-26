#!/usr/bin/python
# -*- coding: utf-8 -*-
'''ares_importer

Package to help to the data files import process'''

from shutil import copy

import os, sys
import logging
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
# Class: Importer
#----------------------------------------------------------------------------
class Importer(object):
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

    def __init__(self, data_dir=None, input_file=None, def_file=None,
                 ares_runtime=None, import_dir=None, data_type=None):
        '''
        Instance initialization method
        '''

        # Get arguments, and incorporate to object
        self.data_dir = data_dir
        self.ares_runtime = Importer.AresRuntimeDir
        self.data_type = data_type
        self.import_dir = None
        self.def_file = def_file
        self.input_file = input_file

        self.num_of_files = 1
        self.num_of_imported_files = 0
        self.num_of_failed_files = 0


        if (not data_type) and (not import_dir):
            this_script_dir = os.path.dirname(os.path.realpath(__file__))
            cfg_file = this_script_dir + '/' + Importer.AresFileTypesCfgFile
            logging.info('Reading import script config. file {}'.format(cfg_file))
            try:
                self.ares_data_types = json.load(open(cfg_file))
                self.compile_patterns()
            except:
                logging.fatal('Import script config. file not found in {}'
                              .format(cfg_file))

        logging.info('-'*60)

        if ares_runtime:
            self.ares_runtime = ares_runtime

        if not os.path.isdir(self.ares_runtime):
            logging.fatal('ARES system runtime folder {} does not exist'
                          .format(self.ares_runtime))
            os._exit(1)

        logging.info('ARES system runtime folder is {}'.format(self.ares_runtime))
        self.ares_import = self.ares_runtime + '/import'
        logging.info('ARES import folder is {}'.format(self.ares_import))

        self.admin_server_log = self.ares_runtime + '/AdminServer/ares_server.log'
        logging.info('Monitoring ARES Server log file {}'.format(self.admin_server_log))

        # Evaluate input data files
        if not os.path.isdir(self.data_dir):
            logging.fatal('Specified input data folder {} does not exist'
                          .format(self.data_dir))
            os._exit(1)

        self.input_files = glob.glob(self.data_dir + '/*.dat')
        logging.debug(self.input_files)
        if len(self.input_files) < 1:
            logging.fatal('No data files found for ingestion')
            os._exit(1)

        if import_dir:
            if not os.path.isdir(import_dir):
                logging.fatal('Location for importing input files {} does not exist'
                              .format(import_dir))
                os._exit(1)
            self.import_dir = import_dir

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

    def wait_until_import_is_successful(self):
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
                last_2_lines = self.tail(f, lines = 2)
                line1 = last_2_lines[0]
                line2 = last_2_lines[1]

                if re.match(r'FileManagerImpl \- Finished importing', line2):
                    if re.match(r' \- Import time:', line1):
                        result = True
                        end_monitor = True
                    if re.match(r' \- Import of task .* failed', line1):
                        result = False
                        end_monitor = True
                    continue

                # Otherwise, keep monitoring

        return result

    def import_definitions(self):
        '''
        Makes an import of a CSV definition file.
        It is assumed that the 'paramdef' part in the import folder name is missing
        '''
        if not self.import_dir:
            logging.fatal('Import folder for definition file is missing!')
            os._exit(1)

        fimport_dir = 'paramdef/' + self.import_dir
        logging.info('Import folder for definition file is {}'.format(fimport_dir))

        fname = self.def_file
        logging.info('Preparing import of definition file: {}'
                     .format(fname))

        import_dir = self.ares_import + '/' + fimport_dir
        logging.info('Data type: {} (folder: {})'.format('DEF_FILE',import_dir))

        # Copy def file to import folder
        copy(fname, import_dir)

        if not self.wait_until_import_is_successful():
            logging.fatal('Import of definition file failed. Exiting.')
            os._exit(1)

        self.import_dir = 'parameter/' + self.import_dir

    def update_stats_on_result(self, result):
        '''
        Updates number of files
        '''
        if result:
            self.num_of_imported_files += 1
            logging.info('Data file imported successfully')
        else:
            self.num_of_failed_files += 1
            logging.warn('Data file importing failed!')

    def do_import_from_dir(self):
        '''
        Loop over input files, to import each of them sequentally
        '''

        # Main loop on files
        self.num_of_files = len(self.input_files)
        for i, fname in enumerate(self.input_files):
            logging.info('Preparing import of file {} of {}: {}'
                         .format(i + 1, self.num_of_files, fname))

            # Detect data type
            ftype = None
            fimport_dir = ''
            if self.import_dir:
                ftype = '<assumed from specified folder>'
                fimport_dir = self.import_dir
            else:
                if self.data_type:
                    ftype = self.data_type
                    fimport_dir = self.ares_data_types[ftype]['dir']
                else:
                    for typ, typ_info in self.ares_data_types.items():
                        rx = typ_info['re']
                        if rx.match(os.path.basename(fname)):
                            ftype = typ
                            fimport_dir = typ_info['dir']
                            break

            if not ftype:
                logging.warn('Unidentified data file type. Failed import.')
                self.num_of_failed_files += 1
                continue

            import_dir = self.ares_import + "/" + fimport_dir
            logging.info('Data type: {} (folder: {})'.format(ftype,import_dir))

            # Copy data file to import folder
            copy(fname, import_dir)

            # Wait for the result
            import_result = self.wait_until_import_is_successful()
            self.update_stats_on_result(import_result)

    def do_import_single_file(self):
        '''
        Loop over input files, to import each of them sequentally
        '''

        fname = self.input_file
        logging.info('Preparing import of file {} of {}: {}'
                     .format(1, self.num_of_files, fname))

        # Detect data type
        ftype = None
        fimport_dir = ''
        if self.import_dir:
            ftype = '<assumed from specified folder>'
            fimport_dir = self.import_dir
        else:
            if self.data_type:
                ftype = self.data_type
                fimport_dir = self.ares_data_types[ftype]['dir']
            else:
                for typ, typ_info in self.ares_data_types.items():
                    rx = typ_info['re']
                    if rx.match(os.path.basename(fname)):
                        ftype = typ
                        fimport_dir = typ_info['dir']
                        break

        if not ftype:
            logging.warn('Unidentified data file type. Failed import.')
            self.num_of_failed_files += 1
            return

        import_dir = self.ares_import + "/" + fimport_dir
        logging.info('Data type: {} (folder: {})'.format(ftype,import_dir))

        # Copy data file to import folder
        copy(fname, import_dir)

        # Wait for the result
        import_result = self.wait_until_import_is_successful()
        self.update_stats_on_result(import_result)

    def run_import(self):
        '''
        Execute import, for one single file or an entire directory, if specified
        '''

        self.compile_patterns()

        if self.def_file:
            self.import_definitions()

        logging.info('Import process starting')
        logging.info('-'*60)

        if self.data_dir:
            self.do_import_from_dir()
        else:
            self.do_import_single_file()

        logging.info('-'*60)
        logging.info('Import process completed.')
        logging.info('{} of {} files successfully imported.'
                     .format(self.num_of_imported_files, self.num_of_files))
        if self.num_of_failed_files > 0:
            logging.warn('{} of {} files import failed.'
                         .format(self.num_of_failed_files, self.num_of_files))
        logging.info('-'*60)
        logging.info('Done.')


def main():
    '''
    Main processor program
    '''
    importer = Importer()


if __name__ == "__main__":
    main()

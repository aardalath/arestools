#!/usr/bin/python
# -*- coding: utf-8 -*-
'''ImportFiles.py

Import data files to ARES

Usage: ImportFiles.py [-h] [-i INPUT] [-f INPUT_FILE] [-D DEF] [-r RUNTIME]
                      [-d DIR] [-t TYPE]

optional arguments:
  -h, --help                        show this help message and exit
  -i INPUT, --input INPUT           Data files directory
  -f INPUT_FILE, --file INPUT_FILE  Input data file
  -D DEF, --def DEF                 Definition file (this makes -d option
                                    mandatory)
  -r RUNTIME, --runtime RUNTIME     ARES Runtime Folder
  -d DIR, --dir DIR                 Import subdirectory to inject the input files
  -t TYPE, --type TYPE              Files data type
where:

  INPUT      Directory where the input data file to import are located
  INPUT_FILE Single input file to be imported
  DEF        Definition file for user-defined data types
  RUNTIME    Directory where the ARES runtime environment is installed
             If non present, then ~/ARES_RUNTIME is assumed, unless
             the env. var. ARES_RUNTIME is set.
  DIR        Subdirectory of ARES_RUNTIME/import where the input files
             have to be placed for import
  TYPE       Assumed data type for all the files

Note that when specifying a definition file, the "paramdef|parameter" part of
the import folder must be omitted.

Alternatively, you can activate the execution permissions of this script, and
call it directly.

Usage example:

  $ python src/ImportFiles.py --dir $(pwd)/in --runtime $(pwd)/runtime

'''

from ares_import.ares_import import Importer

import sys
import logging
import argparse


VERSION = '0.0.1'

__author__ = "jcgonzalez"
__version__ = VERSION
__email__ = "jcgonzalez@sciops.esa.int"
__status__ = "Prototype" # Prototype | Development | Production


# Change INFO for DEBUG to get debug messages
log_level = logging.INFO

# Set up logging information
format_string = '%(asctime)s %(levelname).1s %(message)s'
logging.basicConfig(level=log_level, format=format_string, stream=sys.stderr)

def get_args():
    '''
    Function for parsing command line arguments
    '''
    parser = argparse.ArgumentParser(description='Import data files into ARES',
                                     formatter_class=lambda prog:
                                     argparse.HelpFormatter(prog,
                                                            max_help_position=76),
                                     epilog='Note that when specifying a definition '
                                            'file, the "paramdef|parameter" part of the '
                                            'import folder must be omitted.')
    parser.add_argument('-i', '--input',
                        help='Input data files directory',
                        dest='input', default=None)
    parser.add_argument('-f', '--file',
                        help='Input data file',
                        dest='ifile', default=None)
    parser.add_argument('-D', '--def',
                        help='Definition file (this makes -d option mandatory)',
                        dest='defn', default=None)
    parser.add_argument('-r', '--runtime',
                        help='ARES Runtime Folder',
                        dest='runtime', default=None)
    parser.add_argument('-d', '--dir',
                        help='Import subdirectory to inject the input files',
                        dest='dir', default=None)
    parser.add_argument('-t', '--type',
                        help='Files data type',
                        dest='type', default=None)

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
    args = get_args()

    greetings()

    importer = Importer(data_dir=args.input, input_file=args.ifile, def_file=args.defn,
                        import_dir=args.dir, ares_runtime=args.runtime, data_type=args.type)
    importer.run_import()


if __name__ == "__main__":
    main()

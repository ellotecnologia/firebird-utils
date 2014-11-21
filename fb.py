import sys
import subprocess
from optparse import OptionParser
import drop_views_procedures_triggers

parser = OptionParser(usage="usage: %prog [options] database_path")
parser.add_option('-d', '--drop-sp', dest='drop_sp', 
                  action='store_true',
                  default=False,
                  help='Drop all stored procedures, triggers and views')
(options, args) = parser.parse_args()

if not args:
    print 'Error: Must inform database path!\n'
    print parser.print_help()
    sys.exit(1)
else:
    database_path = args[0]

if options.drop_sp:
    drop_views_procedures_triggers.main(database_path)
#else:
    #subprocess.call(['isql', database])

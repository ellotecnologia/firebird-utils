import sys
import subprocess
import ConfigParser
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-g', '--gui', dest='gui', 
                  action='store_true',
                  default=False,
                  help='Open graphical interface')
(options, args) = parser.parse_args()

iniFile = ConfigParser.RawConfigParser()
iniFile.read('d:/dev/windows/ello.ini')
database = iniFile.get('Dados', 'DataBase').replace('localhost:', '')

if options.gui:
    subprocess.Popen(['C:\\Program Files (x86)\\FlameRobin\\flamerobin.exe', database])
else:
    subprocess.call(['isql', database])

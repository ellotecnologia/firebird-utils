import sys
import itertools

spinner = itertools.cycle(['-', '/', '|', '\\'])

def notify_progress():
    sys.stdout.write(spinner.next())
    sys.stdout.flush()
    sys.stdout.write('\b')

import sys
import itertools

spinner = itertools.cycle(['-', '/', '|', '\\'])

def notify_progress():
    sys.stdout.write(next(spinner))
    sys.stdout.flush()
    sys.stdout.write('\b')

from parsers import StackParser
from sys import argv

if len(argv) < 2:
    raise RuntimeError('a valid stacktrace has to be provided as script argument')
elif len(argv) == 1:
    StackParser.convert_file(argv[1])
else:
    StackParser.convert_files(argv[1:])

# Made at https://github.com/g1thubhub/phil_stopwatch by writingphil@gmail.com
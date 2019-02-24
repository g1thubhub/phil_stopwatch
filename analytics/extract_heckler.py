from parsers import SparkLogParser, AppParser
from typing import Tuple, List, Deque

#################################################################################################

# Collapsed top Logs:
log_file = './data/ProfileHeckler1/JobHeckler1.log.gz'
log_parser = SparkLogParser(log_file)
collapsed_ranked_log: List[Tuple[int, List[str]]] = log_parser.get_top_log_chunks()
for line in collapsed_ranked_log[:5]:  # print 5 most frequently occurring log chunks
    print(line)

#################################################################################################

# Extracting errors from an application:
app_path = './data/application_1549675138635_0005'
app_parser = AppParser(app_path)
app_errors: Deque[Tuple[str, List[str]]] = app_parser.extract_errors()

for error in app_errors:
    print(error)


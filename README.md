# Hive HistoryEvent Log parser
Hive produces a log file with Tez events. Each entry (line) is a single event, formatted
as JSON document. This parser iterates through these log files and searches for the "task
finished" events. For each of these (successful) task finished events, it produces one line
of output in a CSV file, describing the task with some counter metrics, enriched with information
like the LLAP host name.

The parser has support to filter out everything but LLAP Map tasks as these were of special
interest for my tests. This filtering can be enabled via the `-fm` option. The ouput CSV file
uses the `|` symbol as separator by default but this can be overwritten via the `-sm` option
as well.

```
usage: eventhistparser.py [-h] [--separator SEPARATOR] [--filterMap]
                          inDir outFile

positional arguments:
  inDir                  Input directory with history event files
  outFile                File name for the target CSV file

optional arguments:
  --separator SEPARATOR, Optional separator string
  -s SEPARATOR

  --filterMap, -fm       Look for LLAP/Map tasks only```

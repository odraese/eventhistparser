 #!/usr/bin/env python

import os
import json
import argparse

class EventHistParser:
  '''Helper class to keep all the state of parsing through history events.
     A single instance if this parser class is used to scan through the content
     of multiple history event files. It tracks the current state (i.e. known
     tasks and vertex identifiers) and produces the content of the output file.
  '''

  def __init__(self, sep, target, filterMap):
    '''Constructs a new parser instance.
       The so created instance can be use across multiple input files.
    '''

    # Define, which counters (of which groups) we actually want to have in our output CSV
    # the groupName and column properties refer to the counter information in the otherInfo
    # section of a task_finish event.
    self.EXTRACTINFO = {
      'fs': {
        'groupName': 'org.apache.tez.common.counters.FileSystemCounter',
        'columns': ['FILE_BYTES_WRITTEN', 'HDFS_BYTES_READ', 'HDFS_READ_OPS']
      },

      'task': {
        'groupName': 'org.apache.tez.common.counters.TaskCounter',
        'columns': ['INPUT_RECORDS_PROCESSED', 'INPUT_SPLIT_LENGTH_BYTES', 'OUTPUT_RECORDS', 'OUTPUT_BYTES',
                    'OUTPUT_BYTES_WITH_OVERHEAD', 'OUTPUT_BYTES_PHYSICAL']
      },

      'llapIO' : {
        'groupName': 'org.apache.hadoop.hive.llap.counters.LlapIOCounters',
        'columns': [ 'ALLOCATED_BYTES', 'ALLOCATED_USED_BYTES', 'CACHE_MISS_BYTES', 'CONSUMER_TIME_NS', 'DECODE_TIME_NS',
                     'HDFS_TIME_NS', 'IO_CPU_NS', 'IO_USER_NS', 'METADATA_CACHE_MISS', 'NUM_DECODED_BATCHES',
                     'NUM_VECTOR_BATCHES', 'ROWS_EMITTED', 'SELECTED_ROWGROUPS', 'TOTAL_IO_TIME_NS' ]
      },

      'llapQueue' : {
        'groupName': 'org.apache.hadoop.hive.llap.counters.LlapWmCounters',
        'columns': [ 'SPECULATIVE_QUEUED_NS', 'SPECULATIVE_RUNNING_NS', 'GUARANTEED_QUEUED_NS', 'GUARANTEED_RUNNING_NS']
      }
    }

    # Separator between fields in the CSV output
    self.separator = sep
    self.targetFile = target

    # program statistics
    self.taskStats = {
      'killedTasks': 0,             # overall task_finish events with state killed
      'killedBecauseBusy': 0,       # subset of killed with reason 'busy'
      'succeededMap': 0,            # successfully finished Map tasks (got a node)
      'succeededOther': 0           # all other successfully (Reducer) tasks
    }

    self.openAttempts = {}          # currently known, valid task attempts (from task_attempt_start)
    self.knownVertexes = set()      # set of all vertex IDs, for LLAP Map tasks (from vertex_init)
    self.knownMapTasks = set()      # set of all tasks, belonging to LLAP Map vertices (from task_start)
    self.firstRow = True            # first row needs to dump header line for CSV
    self.taskLoop = 0               # total amount of processed events
    self.parseErrors = -1           # keep track of JSON parsing errors
    self.filterLLAPMap = filterMap  # filter for LLAP Map tasks

  def readEventFile(self, filename):
    '''Processes a single event file, writing the CSV to the target
       The function reads the JSON events from the via first parameter specified
       file and writes a line to the targetName CSV file for each successful LLAP
       Map task that finished.
    '''
    pathElements = filename.split('/')
    shortFile = pathElements[-1]

    # open JSON history event file and the target output CSV
    with open(filename, 'r') as f:
      with open(self.targetFile, 'a') as t:
        for line in f:                 # each line is an event
          while line[-1] != '}':       # remove all wireds line separators
            line = line[0:-1]

          try:
            entry = json.loads(line)              # parse the JSON
            self.handleEntry(shortFile, entry, t) # pass the JSON to the event processor
          except Exception as parseErr:
            if self.parseErrors < 0:
              print('Error parsing input file: {}'.format(parseErr))
              exit(8)

            self.parseErrors += 1                 # parsing JSON failed

  def isEventType(self, entry, typeName):
    '''Helper to figure out if an entry (JSON object as dictionary) represents
       a particular task event, specified via the second parameter.
    '''
    if 'events' in entry:
      for e in entry['events']:
        if 'eventtype' in e:
          if e['eventtype'] == typeName:
            return True

    return False

  def isVertexInit(self, entry):
    '''Returns True if the passed in event is a VERTEX_INITIALIZED event'''
    return self.isEventType(entry, 'VERTEX_INITIALIZED')

  def isTaskStart(self, entry):
    '''Returns True if the passed in event is a TASK_STARTED event'''
    return self.isEventType(entry, 'TASK_STARTED')

  def isTaskAttempt(self, entry):
    '''Returns True if the passed in event is a TASK_ATTEMPT_STARTED event'''
    return self.isEventType(entry, 'TASK_ATTEMPT_STARTED')

  def isTaskFinish(self, entry):
    '''Returns True if the passed in event is a TASK_ATTEMPT_FINISHED event'''
    return self.isEventType(entry, 'TASK_ATTEMPT_FINISHED')

  def isLLAPMapTask(self, entry):
    '''Returns True if the passed in event is representing a Map task, that is
       scheduled on an LLAP node. This program filters all tasks by LLAP Map
       tasks. This function is called on VERTEX_INITIALIZED events, which contain
       the vertex name (to figure out task type) and scheduler plugin.
    '''
    if 'otherinfo' in entry:
      otherInfo = entry['otherinfo']
      if 'vertexName' in otherInfo and otherInfo['vertexName'].startswith('Map'):
        if 'servicePlugin' in otherInfo:
          svcPlugin = otherInfo['servicePlugin']
          if 'taskSchedulerName' in svcPlugin and svcPlugin['taskSchedulerName'] == 'LLAP':
            return True

    return False

  def getNode(self, entry):
    '''Returns the node name for a task finsish event if the task is known.
       The node name comes originally from task_attempt_start events and is stored
       only for tasks, which qualify the LLAP/Map filter. If this method returns
       the node (called on task_finish), the task was a LLAP/Map, statted on the
       returned node. Otherwise, the function returns an empty string, meaning
       that this was the task_finish event of an unknown or filtered task.
    '''
    ret = ''

    if 'entitytype' in entry and entry['entitytype'] == 'TEZ_TASK_ATTEMPT_ID':
      attemptID = entry['entity']

      if attemptID in self.openAttempts:    # should be in openAttempts for LLAP/Map tasks
        ret = self.openAttempts[attemptID]
        del self.openAttempts[attemptID]    # remove it - no longer needed in the dictionary

    return ret

  def getCounterGroup(self, otherInfo, groupID, useValue):
    '''Returns the counter titles or values as CSV string.
       This function is used to extract the counters for a particular counter
       group (as defined by EXTRACTINFO above) from the task_finish event and to
       deliver them as a CSV string if useValue is True or as title strings if
       useValue is False.
    '''
    ret = ''

    if 'counters' in otherInfo and 'counterGroups' in otherInfo['counters']:
      groups = otherInfo['counters']['counterGroups']

      # get the expected counter spec from EXTRACTINFO for the specified groupID
      groupTemplate = self.EXTRACTINFO[groupID]
      groupName = groupTemplate['groupName']

      # search through all counter groups in the event result
      foundGroup = False
      for g in groups:
        if 'counterGroupName' in g and g['counterGroupName'] == groupName:
          foundGroup = True
          counters = g['counters']

          if not isinstance(counters, list):
            counters = [counters]

          # format out counter by counter within that group
          for counterTemplate in groupTemplate['columns']:
            if useValue:
              foundCounter = False

              for c in counters:
                if c['counterName'] == counterTemplate:
                  ret += '{}{}'.format(self.separator, c['counterValue'])
                  foundCounter = True
                  break

              # expected counter not found on event data, just produce separator
              if not foundCounter:
                ret += self.separator
            else:
              ret += '{}{}'.format(self.separator, counterTemplate)

          break

      # whole counter group missing in task_finish event? just produce separators
      if not foundGroup:
        for counterTemplate in groupTemplate['columns']:
          ret += self.separator

    return ret

  def dumpProgress(self, fileName):
    '''Writes the current progress with stats to the console.'''
    if self.parseErrors < 0:
      print('{}: {}: {} - with {} open attempts'.format(fileName, self.taskLoop,
                                                        self.taskStats, len(self.openAttempts)))
    else:
      print('{}: {}: {} - with {} open attempts ({} errors)'.format(fileName, self.taskLoop, self.taskStats,
                                                                    len(self.openAttempts), self.parseErrors))

  def handleEntry(self, sourceFileName, entry, targetFile):
    ''' Processor for a single task event.
        This function is called for each of the event, found in any of the input
        files. The sourceFileName is used for progress output only, while the
        entry parameter has to be the parsed JSON task event (as dictionary).

        The function handles
        - VERTEX_INIT events to figure out if the task is a Map task, running on
          LLAP. It stores the vertexID in knownVertexes.
        - TASK_STARTED events to map vertexID to taskID (stored in knownMapTasks)
        = TASK_ATTEMPT_START to store the nodeID (stored in openAttempts)
        - TASK_FINISHED to finally write out the counters of a successful task
    '''
    if self.isVertexInit(entry):
      if not self.filterLLAPMap or self.isLLAPMapTask(entry):
        # vertexID is stored in knownVertexes for LLAP/Map tasks only
        if 'entitytype' in entry and entry['entitytype'] == 'TEZ_VERTEX_ID' and 'entity' in entry:
          self.knownVertexes.add(entry['entity'])
    elif self.isTaskStart(entry):
      if 'relatedEntities' in entry and 'entity' in entry:
        vertexID = '<Unknown>'

        # extract the vertexID from the TASK_STARTED event
        for relEnt in entry['relatedEntities']:
          if 'entitytype' in relEnt and relEnt['entitytype'] == 'TEZ_VERTEX_ID' and 'entity' in relEnt:
            vertexID = relEnt['entity']
            break

        # does the vertexID belong to a known LLAP/Map task? (store the taskID)
        if vertexID in self.knownVertexes:
          self.knownMapTasks.add(entry['entity'])
    elif self.isTaskAttempt(entry):
      if 'entitytype' in entry and entry['entitytype'] == 'TEZ_TASK_ATTEMPT_ID' and 'entity' in entry and 'relatedEntities' in entry:
        relEnt = entry['relatedEntities']
        isKnownTask = False

        # extract the task ID and chedk that it belongs to a LLAP/Map task
        for e in relEnt:
          if 'entitytype' in e and e['entitytype'] == 'TEZ_TASK_ID':
            taskID = e['entity']
            if taskID in self.knownMapTasks:
              isKnownTask = True
              break

        if isKnownTask:
          attemptID = entry['entity']
          if 'relatedEntities' in entry:
            for ent in entry['relatedEntities']:
              if 'entitytype' in ent and ent['entitytype'] == 'nodeId':
                # extract the nodeID (where task is started) and store it
                nodeID = ent['entity']
                nodeID = nodeID[0:nodeID.index('.')]
                self.openAttempts[attemptID] = nodeID
                break
    elif self.isTaskFinish(entry):
      node = self.getNode(entry)   # a nodeID != '' means that this is a LLAP/Map task

      if 'otherinfo' in entry:
        other = entry['otherinfo']

        # check if the task was executed successful or was killed....
        if 'status' in other:
          if other['status'] == 'KILLED':
            self.taskStats['killedTasks'] += 1
            if 'taskAttemptErrorEnum' in other:
              if other['taskAttemptErrorEnum'] == 'SERVICE_BUSY':
                self.taskStats['killedBecauseBusy'] += 1
          elif other['status'] == 'SUCCEEDED':
            if node != '':
              self.taskStats['succeededMap'] += 1

              timeStamp = entry['events'][0]['ts']
              timeTaken = other['timeTaken']

              if self.firstRow:
                # write header line to output (with all counter names)
                self.firstRow = False
                outLine = 'TIMESTAMP{}TIME_TAKEN{}NODE'.format(self.separator, self.separator)
                for cgn in self.EXTRACTINFO.keys():
                  outLine += self.getCounterGroup(other, cgn, False)

                targetFile.write(outLine + '\n')

              # write event counters to output as CSV
              outLine = '{}{}{}{}{}'.format(timeStamp, self.separator, timeTaken, self.separator, node)
              for cgn in self.EXTRACTINFO.keys():
                outLine += self.getCounterGroup(other, cgn, True)

              targetFile.write(outLine + '\n')
            else:
              self.taskStats['succeededOther'] += 1
        else:
          print(entry)

    # every 10000 events, we output a progress information line
    self.taskLoop += 1
    if self.taskLoop % 10000 == 0:
      self.dumpProgress(sourceFileName)


# Program execution starts here
# ---------------------------------------------------------------------------------------
aParser = argparse.ArgumentParser(description='Parse Hive HistoryEvent logs.')
aParser.add_argument('inDir', help='Input directory with history event files', type=str)
aParser.add_argument('outFile', help='File name for the target CSV file', type=str)
aParser.add_argument('--separator', '-s', help='Optional separator string', type=str)
aParser.add_argument('--filterMap', '-fm', action='store_true', help='Look for LLAP/Map tasks only')
aParser.add_argument('--skipErrors', '-e', action='store_true', help='Continue if input parsing errors are found.')

args = aParser.parse_args()
if os.path.isdir(args.inDir):
  inputDirectory = args.inDir
  if not inputDirectory.endswith('/'):
    inputDirectory += '/'
else:
  print('Input directory {} does not exist', args.inDir)
  exit(4)

if os.path.exists(args.outFile):
  os.remove(args.outFile)

outputFile = args.outFile

if args.separator != None:
  separator = args.separator.strip()
else:
  separator = '|'

try:
  parser = EventHistParser(separator, outputFile, args.filterMap)
  allFiles = []

  # continue on parsing errors
  if args.skipErrors:
    parser.parseErrors = 0

  # create (sorted) input file list
  for filename in os.listdir(inputDirectory):
    allFiles.append(inputDirectory + filename)

  allFiles.sort()

  # parse input files sequentially
  for fn in allFiles:
    parser.readEventFile(fn)

  parser.dumpProgress('--End--')
except Exception as e:
  print(e)
  exit(8)

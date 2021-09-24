import csv
import os

from celery import group, chord

from core.model.CeleryConfiguration import app
from nepac.model.NepacProcess import NepacProcess


# -----------------------------------------------------------------------------
# class NepacProcessCelery
# -----------------------------------------------------------------------------
class NepacProcessCelery(NepacProcess):

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self, nepacInputFile, missionDataSetDict, outputDir,
                 dummyPath, noData, erroredData):

        super(NepacProcessCelery, self).__init__(nepacInputFile,
                                                 missionDataSetDict,
                                                 outputDir,
                                                 dummyPath,
                                                 noData=noData,
                                                 erroredData=erroredData)
        self._dummyPath = dummyPath
        self._outputDir = outputDir
        self._validateMissionDataSets(missionDataSetDict)
        self._missions = missionDataSetDict

    # -------------------------------------------------------------------------
    # run
    # -------------------------------------------------------------------------
    def run(self):

        # Read the input file and aggregate by mission.
        timeDateLocToChl = self._readInputFile()

        # Write the output file.
        outFileName = os.path.splitext(
            os.path.basename(self._inputFile.fileName()))
        outFileName = outFileName[0] + \
            self.RESULT_APPEND_STRING + \
            outFileName[1]
        outputFile = os.path.join(self._outputDir,
                                  outFileName)
        self._initializeCSV(outputFile)

        chunkedDict = self._splitDict(timeDateLocToChl,
                                      NepacProcess.CHUNK_SIZE)
        numChunks = len(chunkedDict)
        for i, chunk in enumerate(chunkedDict):
            print('Processing chunk {} of {}'.format(i+1, numChunks))
            self._process(chunk, outputFile)
        NepacProcess.removeNCFiles()

    # -------------------------------------------------------------------------
    # process
    #
    # See NepacProcess.process() for detailed comments.
    #
    # In order to keep from deadlocks, NepacProcessCelery._processMission()
    # is called directly from process through a Celery chord, with
    # NepacProcessCelery._processTimeDate() as the callback function.
    # This allows all workers to be safely spawned
    # to run _processMission() for each mission to get pixel data from. Once
    # complete, _processTimeDate takes the aggregated results and processes
    # the data to a dictionary.
    #
    # The chords mentioned above are spawned asynchronously through a Celery
    # group where a chord is made for each time-date-loc present in the input
    # file.
    # -------------------------------------------------------------------------
    def _process(self, timeDateLocToChl, outputFile):
        # Get the pixel values for each mission.
        rowsToWrite = []

        chordPerTimeDateLoc = group([
            chord([NepacProcessCelery._processMission.s(
                mission,
                timeDateLoc,
                timeDateLocToChl[timeDateLoc],
                self._missions,
                self._outputDir,
                self._dummyPath,
                noDataValue=self._noData,
                erroredDataValue=self._erroredData)
                for mission in self._missions],
                NepacProcessCelery._processTimeDate.s(
                timeDate=timeDateLoc,
                locsChls=timeDateLocToChl[timeDateLoc],
                missions=self._missions,
                outputDir=self._outputDir,
                dummyPath=self._dummyPath)) for timeDateLoc in timeDateLocToChl
        ])

        chordPerTimeDateLocResult = chordPerTimeDateLoc.apply_async()
        chordPerTimeDateLocResultProcessed = chordPerTimeDateLocResult.get()

        for timeDateLocDict in chordPerTimeDateLocResultProcessed:

            rowsPerTimeDate = []

            for i, (missionKey, missionVals) in enumerate(timeDateLocDict.
                                                          items()):
                for k, (rowKey, rowValues) in enumerate(missionVals.items()):

                    # First time seeing these keys, new row.
                    if i == 0:
                        newRow = []
                        # Convert keys from a string to a tuple.
                        rowKeyTuple = tuple(rowKey.split(","))
                        newRow.extend(list(rowKeyTuple))
                        newRow.extend(rowValues)
                        rowsPerTimeDate.append(newRow)

                    # Keys are already present, append data.
                    else:
                        rowsPerTimeDate[k].extend(rowValues)

            # Write this timeDate's data rows to the overall rows.
            rowsToWrite.extend(rowsPerTimeDate)

        # Start writing to CSV
        with open(outputFile, 'a') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(rowsToWrite)

    # -------------------------------------------------------------------------
    # processTimeDate
    # -------------------------------------------------------------------------
    @staticmethod
    @app.task(autoretry_for=(Exception,))
    def _processTimeDate(missionDictList,
                         timeDate=None,
                         locsChls=None,
                         missions=None,
                         outputDir=None,
                         dummyPath=None):

        # Add the pixel data in an aggregated per-mission dictionary.
        valuesPerMissionDict = {}
        for perMissionDict in missionDictList:
            valuesPerMissionDict.update(perMissionDict)

        # Sort dictionary keys to match with how Missions are added to csv.
        sortedValuesPerMissionDict = dict(
            sorted(valuesPerMissionDict.items(),
                   key=lambda item: item[0])
        )

        return sortedValuesPerMissionDict

    # -------------------------------------------------------------------------
    # processMission
    # -------------------------------------------------------------------------
    @staticmethod
    @app.task(autoretry_for=(Exception,), retry_backoff=True)
    def _processMission(mission, timeDateLoc, chls, missions, outputDir,
                        dummyPath, noDataValue=9999, erroredDataValue=9998):

        nepacOutput = NepacProcess._processMission(
            mission,
            timeDateLoc,
            chls,
            missions,
            outputDir,
            dummyPath,
            noDataValue=noDataValue,
            erroredDataValue=erroredDataValue)
        return nepacOutput

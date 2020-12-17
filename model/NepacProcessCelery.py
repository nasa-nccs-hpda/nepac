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
    def __init__(self, nepacInputFile, missionDataSetDict, outputDir):

        super(NepacProcessCelery, self).__init__(nepacInputFile,
                                                 missionDataSetDict,
                                                 outputDir)

        self._outputDir = outputDir
        self._validateMissionDataSets(missionDataSetDict)
        self._missions = missionDataSetDict

    # -------------------------------------------------------------------------
    # run
    #
    # See NepacProcess.run() for detailed comments.
    #
    # In order to keep from deadlocks, NepacProcessCelery._processMission()
    # is called directly from run through a Celery chord, with
    # NepacProcessCelery._processTimeDate() as the callback function.
    # This allows all workers to be safely spawned
    # to run _processMission() for each mission to get pixel data from. Once
    # complete, _processTimeDate takes the aggregated results and processes
    # the data to a dictionary.
    #
    # The chords mentioned above are spawned asynchronously through a Celery
    # group where a chord is made for each time-date present in the input
    # file.
    # -------------------------------------------------------------------------
    def run(self):

        # Read the input file and aggregate by mission.
        timeDateToLocChl = self._readInputFile()

        # Get the pixel values for each mission.
        rowsToWrite = []

        chordPerTimeDate = group([
            chord([NepacProcessCelery._processMission.s(
                mission,
                timeDate,
                timeDateToLocChl[timeDate],
                self._missions,
                self._outputDir)
                for mission in self._missions],
                NepacProcessCelery._processTimeDate.s(
                timeDate=timeDate,
                locsChls=timeDateToLocChl[timeDate],
                missions=self._missions,
                outputDir=self._outputDir)) for timeDate in timeDateToLocChl
        ])

        chordPerTimeDateResult = chordPerTimeDate.apply_async()
        chordPerTimeDateResultProcessed = chordPerTimeDateResult.get()

        for timeDateDict in chordPerTimeDateResultProcessed:

            rowsPerTimeDate = []

            for i, (missionKey, missionVals) in enumerate(timeDateDict.
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

        # Write the output file.
        outFileName = os.path.splitext(
            os.path.basename(self._inputFile.fileName()))
        outFileName = outFileName[0] + '_output' + outFileName[1]

        outputFile = os.path.join(self._outputDir,
                                  outFileName)

        # Start writing to CSV
        with open(outputFile, 'w') as csvfile:

            csvwriter = csv.writer(csvfile)

            # Start with base fields
            fields = ['Time[hhmm]',
                      'Date[mmddyyyy]',
                      'Lat.[-90—90 deg.]',
                      'Long.[0-360.E]',
                      'Chl-a']

            # Sort keys in missions to match incoming data, add to fields.
            for mission in sorted(self._missions.keys()):
                for subDataSet in sorted(self._missions[mission]):
                    fields.append(str(mission+'-'+subDataSet))

            csvwriter.writerow(fields)
            csvwriter.writerows(rowsToWrite)

    # -------------------------------------------------------------------------
    # processTimeDate
    # -------------------------------------------------------------------------
    @staticmethod
    @app.task()
    def _processTimeDate(missionDictList,
                         timeDate=None,
                         locsChls=None,
                         missions=None,
                         outputDir=None):

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
    @app.task()
    def _processMission(mission, timeDate, locsChls, missions, outputDir):

        print('In _processMission')

        nepacOutput = NepacProcess._processMission(mission,
                                                   timeDate,
                                                   locsChls,
                                                   missions,
                                                   outputDir)
        return nepacOutput

import argparse
import sys
import os

from nepac.model.ILProcessController import ILProcessController
from nepac.model.NepacProcessCelery import NepacProcessCelery
from nepac.model.NepacProcess import NepacProcess


# -----------------------------------------------------------------------------
# main
#
# python nepac/view/NepacCommandLineView.py --celery -f \
# nepac/model/tests/nepacInputTwo.csv -m 'MODIS-Terra:Rrs_443 MODIS-Aqua:ipar'
# -----------------------------------------------------------------------------
def main():

    desc = 'This application runs NepacProcess'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--celery',
                        action='store_true',
                        help='The option to use celery to distribute' +
                        ' the tasks.')

    parser.add_argument('-f',
                        required=True,
                        help='Path to input file with time, date, location,' +
                        ' and Chl-a values.')

    parser.add_argument('-o',
                        default='.',
                        help='Output directory path.')

    parser.add_argument('-md_file',
                        required=False,
                        type=str,
                        help='Use this if the user would like to give' +
                        ' NEPAC a file with Mission:Dataset on each line.\n' +
                        'Examples of a file would look like: \n' +
                        'MODIS-Terra:Rrs_443 MODIS-Aqua:ipar' +
                        '\n where each Mission:Dataset combo is' +
                        ' a new line.')

    parser.add_argument('-m',
                        required='-md_file' not in sys.argv,
                        type=str,
                        help='Mission:Dataset list to sample pixel values' +
                        'from.\n' +
                        'Example use of this argument looks like: \n' +
                        '\"MODIS-Terra:Rrs_443 MODIS-Aqua:ipar\"\n')

    args = parser.parse_args()

    missionDatasets = []
    if args.m:
        missionDatasets = args.m.split()  # Using CMD line args as input.
    else:
        if os.path.exists(args.md_file):  # Using file as input.
            with open(args.md_file, 'r') as srcFile:
                for line in srcFile:
                    lineStripper = line.strip()
                    missionDatasets.append(lineStripper)
        else:
            msg = '{} not found'.format(args.md_file)
            raise RuntimeError(msg)

    # -------------------------------------------------------------------------
    # Take the Mission:Dataset string from cmd line arguments
    # or file input in the form of:
    # 'Mission1:Subdataset1 Mission1:Subdataset2 Mission2:Subdataset2'
    # and make a dictionary where keys are the Missions and the values per
    # key are Subdatasets:
    # {
    #   'Mission1' : ['Subdataset1', Subdataset2'],
    #   'Mission2' : ['Subdataset1']
    # }
    # -------------------------------------------------------------------------
    missionDataSetDict = {}
    for missionDataset in missionDatasets:

        mission, dataset = missionDataset.split(':')

        if mission not in missionDataSetDict:
            missionDataSetDict[mission] = []

        missionDataSetDict[mission].append(dataset)

    if args.celery:
        with ILProcessController() as processController:
            np = NepacProcessCelery(args.f,
                                    missionDataSetDict,
                                    args.o)
            np.run()
    else:
        np = NepacProcess(args.f,
                          missionDataSetDict,
                          args.o)
        np.run()


if __name__ == "__main__":
    sys.exit(main())

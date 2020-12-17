import argparse
import sys

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

    parser.add_argument('-m',
                        required=True,
                        type=str,
                        help='Mission:Dataset list to sample pixel values' +
                        'from.\n' +
                        'Example use of this argument looks like: \n' +
                        '\"MODIS-Terra:Rrs_443 MODIS-Aqua:ipar\"\n')

    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # Take the Mission:Dataset string from cmd line arguments in the form of:
    # 'Mission1:Subdataset1 Mission1:Subdataset2 Mission2:Subdataset2'
    # and make a dictionary where keys are the Missions and the values per
    # key are Subdatasets:
    # {
    #   'Mission1' : ['Subdataset1', Subdataset2'],
    #   'Mission2' : ['Subdataset1']
    # }
    # -------------------------------------------------------------------------
    missionDataSetDict = {}

    for missionDataset in args.m.split():

        mission, dataset = missionDataset.split(':')

        if mission not in missionDataSetDict:
            missionDataSetDict[mission] = []

        missionDataSetDict[mission].append(dataset)

    if args.celery:

        # Spawn Redis and Celery
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

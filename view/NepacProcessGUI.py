import sys

import tkinter as tk
from tkinter import scrolledtext
from tkinter.constants import LEFT
from tkinter import filedialog

from nepac.model.Retriever import Retriever
from nepac.model.ILProcessController import ILProcessController
from nepac.model.NepacProcessCelery import NepacProcessCelery


# -------------------------------------------------------------------------
# NepacProcessGUI
#
# Graphical User Interface (GUI) as a view for NEPAC Data Retriever
# -------------------------------------------------------------------------
class NepacProcessGUI(tk.Frame):

    ROW_SPAN = 12
    DEFAULT_NO_DATA = -9999
    DEFAULT_ERRORED_DATA = -9998

    TITLE = 'NEPAC Data Retriever'
    GEOMETRY = '2000x1000'
    FILE_PATH_DEFAULT = 'NO CSV FILE SELECTED'
    FILE_BUTTON_TEXT = 'Choose file as input'
    OUTPUT_FOLDER_PATH_DEFAULT = 'NO OUTPUT DIRECTORY SELECTED'
    OUTPUT_FOLDER_BUTTON_TEXT = 'Choose directory as output directory'
    DUMMY_FOLDER_PATH_DEFAULT = '/usr/local/nepac/model/datasets'
    DUMMY_FOLDER_BUTTON_TEXT = 'Choose directory as dummy dataset directory'
    NO_DATA_TEXT = 'No-data value: '
    ERRORED_DATA_TEXT = 'Errored-data value: '
    SUBMIT_JOB_TEXT = 'Run Nepac Process'

    # -------------------------------------------------------------------------
    # __init__
    # -------------------------------------------------------------------------
    def __init__(self, master):

        self.master = master
        tk.Frame.__init__(self, self.master)

        self.row = 0
        self.col = 0

        self.missionInputDict = {}
        self.buttonDict = {}
        self.varDict = {}

        self.MISSION_DICT = Retriever.MISSION_DATASETS

        self._configureGui()
        self._createTitle()

        self._populateButtons()

        self.noData = tk.IntVar(value=self.DEFAULT_NO_DATA)
        self._createNoDataEntry()

        self.erroredData = tk.IntVar(value=self.DEFAULT_ERRORED_DATA)
        self._createErroredPixelEntry()

        self.filePath = tk.StringVar()
        self.filePath.set(self.FILE_PATH_DEFAULT)
        self.filePathButton = self._createFilePath(filePath=self.filePath)
        self._createFileSearch()

        self.outputFilePath = tk.StringVar()
        self.outputFilePath.set(self.OUTPUT_FOLDER_PATH_DEFAULT)
        self.outputFilePathButton = self._createFilePath(
            filePath=self.outputFilePath)
        self._createOutputFolderSearch(
            buttonText=self.OUTPUT_FOLDER_BUTTON_TEXT)

        self.dummyDataPath = tk.StringVar()
        self.dummyDataPath.set(self.DUMMY_FOLDER_PATH_DEFAULT)
        self.dummyDataPathButton = self._createFilePath(
            filePath=self.dummyDataPath
        )
        self._createDummyFolderSearch(buttonText=self.DUMMY_FOLDER_BUTTON_TEXT)

        self.win = self._addLogWindow()
        sys.stdout = self.win.log

        self._submitJob()

    # -------------------------------------------------------------------------
    # _configureGui()
    #
    # Add title and make window to certain size.
    # -------------------------------------------------------------------------
    def _configureGui(self):
        self.master.title(self.TITLE)
        self.master.geometry(self.GEOMETRY)

    # -------------------------------------------------------------------------
    # _createTitle()
    #
    # Create a title for top of GUI.
    # -------------------------------------------------------------------------
    def _createTitle(self):
        title = tk.Label(master=self.master, text=self.TITLE)
        title.grid(column=self.col, row=self.row)
        self.row += 1

    # -------------------------------------------------------------------------
    # _createFilePath()
    #
    # Create a label that shows the file selected for input and output.
    # -------------------------------------------------------------------------
    def _createFilePath(self, filePath='.'):
        filePathButton = tk.Label(master=self.master,
                                  textvariable=filePath)
        filePathButton.grid(columnspan=3,
                            column=4,
                            row=self.row)
        return filePathButton

    # -------------------------------------------------------------------------
    # _createFileSearch()
    #
    # A button which spawns an open file dialog to choose the input file.
    # -------------------------------------------------------------------------
    def _createFileSearch(self):
        buttonToOpenFile = tk.Button(master=self.master,
                                     text=self.FILE_BUTTON_TEXT,
                                     command=self._openFileDialog)
        buttonToOpenFile.grid(column=0, row=self.row)
        self.row += 1

    # -------------------------------------------------------------------------
    # _createOutputFolderSearch()
    #
    # Similar to _createFileSearch but with an open directory dialog as output.
    # -------------------------------------------------------------------------
    def _createOutputFolderSearch(self, buttonText):
        buttonToOpenFolder = tk.Button(master=self.master,
                                       text=buttonText,
                                       command=self._openOutputFolderDialog)
        buttonToOpenFolder.grid(column=0, row=self.row)
        self.row += 1

    # -------------------------------------------------------------------------
    # _createDummyFolderSearch()
    #
    # Similar to _createFileSearch but with an open directory dialog as output.
    # -------------------------------------------------------------------------
    def _createDummyFolderSearch(self, buttonText):
        buttonToOpenFolder = tk.Button(master=self.master,
                                       text=buttonText,
                                       command=self._openDummyFolderDialog)
        buttonToOpenFolder.grid(column=0, row=self.row)
        self.row += 1
    # -------------------------------------------------------------------------
    # _openFileDialog()
    # -------------------------------------------------------------------------

    def _openFileDialog(self):
        self.filePath.set(filedialog.askopenfilename())

    # -------------------------------------------------------------------------
    # _openOutputFolderDialog()
    # -------------------------------------------------------------------------
    def _openOutputFolderDialog(self):
        self.outputFilePath.set(filedialog.askdirectory())

    # -------------------------------------------------------------------------
    # _openDummyFolderDialog()
    # -------------------------------------------------------------------------
    def _openDummyFolderDialog(self):
        self.dummyDataPath.set(filedialog.askdirectory())

    # -------------------------------------------------------------------------
    # _populateButtons()
    #
    # For each mission and each subdataset in each mission. List each
    # subdataset as a checkbutton for users to choose which subdataset they
    # want.
    # -------------------------------------------------------------------------
    def _populateButtons(self):
        row = self.row
        col = self.col

        for instrument, subDatasets in self.MISSION_DICT.items():

            col = 0
            mission = tk.Label(master=self.master,
                               text=instrument, justify=LEFT)
            mission.grid(column=col, row=row)
            col = 1
            subDatasetList = []
            varList = []
            for subDataset in subDatasets:
                if col % self.ROW_SPAN == 0:
                    row += 1
                    col = 1
                chk, chk_state = self._createSubdatasetButton(subDataset,
                                                              col,
                                                              row)
                subDatasetList.append(chk)
                varList.append(chk_state)
                col += 1
            row += 2
            self.buttonDict[instrument] = subDatasetList
            self.varDict[instrument] = varList
        self.row = row
        self.col = col

    # -------------------------------------------------------------------------
    # _createSubdatasetButton()
    #
    # Helper function to create a checkbox button.
    # -------------------------------------------------------------------------
    def _createSubdatasetButton(self, text, col, row):
        chk_state = tk.IntVar()
        chk_state.set(False)
        chk = tk.Checkbutton(master=self.master,
                             text=text,
                             var=chk_state,
                             justify=LEFT,
                             onvalue=1,
                             offvalue=0,
                             command=self._onCheckButtonPress)
        chk.grid(column=col, row=row, padx=(5, 5), pady=(5, 5))
        return chk, chk_state

    # -------------------------------------------------------------------------
    # _onCheckButtonPress()
    #
    # If a button is selected, add that subdataset to the list of subdatasets
    # to the Mission:Subdataset directory for NepacProcess input. If a button
    # is deselected, ensure that the corresponding subdataset is removed from
    # the NepacProcess Mision:Subdataset input.
    # -------------------------------------------------------------------------
    def _onCheckButtonPress(self):
        keys = list(self.varDict.keys())
        vals = list(self.varDict.values())
        for val in vals:
            for v in val:
                k = keys[vals.index(val)]
                idx = val.index(v)
                subDataset = self.buttonDict[k][idx].cget('text')
                if v.get() == 1:
                    if k not in self.missionInputDict:
                        self.missionInputDict[k] = set()
                    self.missionInputDict[k].add(subDataset)
                elif v.get() == 0:
                    if k in self.missionInputDict:
                        if subDataset in self.missionInputDict[k]:
                            self.missionInputDict[k].remove(subDataset)
                        if len(self.missionInputDict[k]) == 0:
                            self.missionInputDict.pop(k)
        print('Current selection: \n {} \n'.format(nepacGUI.missionInputDict))

    # -------------------------------------------------------------------------
    # _createNoDataEntry()
    #
    # A box which takes user input on what they want the no-data value to be.
    # No-data vals are the result of a pixel reading that is masked.
    # -------------------------------------------------------------------------
    def _createNoDataEntry(self):
        noDataLabel = tk.Label(master=self.master, text=self.NO_DATA_TEXT)
        noDataEntry = tk.Entry(master=self.master, textvariable=self.noData)
        noDataLabel.grid(column=5, row=self.row)
        noDataEntry.grid(column=6, row=self.row)

    # -------------------------------------------------------------------------
    # _createNoDataEntry()
    #
    # A box which takes user input on what they want the errored-pixel value
    # to be. Errored-pixel vals are the result of various reasouns. Sme being:
    # - HTTP Error.
    # - No observation occures for the requested time/date/location.
    # - No observation occurs within spatial window.
    # -------------------------------------------------------------------------
    def _createErroredPixelEntry(self):
        erroredDataLabel = tk.Label(master=self.master,
                                    text=self.ERRORED_DATA_TEXT)
        erroredDataEntry = tk.Entry(master=self.master,
                                    textvariable=self.erroredData)
        erroredDataLabel.grid(column=7, row=self.row)
        erroredDataEntry.grid(column=8, row=self.row)
        self.row += 1

    # -------------------------------------------------------------------------
    # _submitJob()
    #
    # A button which on click, runs NepacProcess.
    # -------------------------------------------------------------------------
    def _submitJob(self):
        self.row += 1
        buttonToRunNepac = tk.Button(master=self.master,
                                     text=self.SUBMIT_JOB_TEXT,
                                     command=self._runNepac)
        buttonToRunNepac.grid(column=self.col, row=self.row)
        self.row += 1

    # -------------------------------------------------------------------------
    # runNepac()
    #
    # Outputs current selection, runs parallel implementation of NepacProcess.
    # -------------------------------------------------------------------------
    def _runNepac(self):
        self._printSelections()
        for k, v in self.missionInputDict.items():
            self.missionInputDict[k] = list(v)
        with ILProcessController() as processController:
            try:
                nepacProcess = NepacProcessCelery(
                    self.filePath.get(),
                    self.missionInputDict,
                    outputDir=self.outputFilePath.get(),
                    dummyPath=self.dummyDataPath.get(),
                    noData=self.noData.get(),
                    erroredData=self.erroredData.get())
                nepacProcess.run()
            except Exception as e:
                errorStr = 'Encountered error: {}.'.format(e) +\
                    'Shutting down workers.'
                print(errorStr)
        print('Finished job, you may exit this application.')

    # -------------------------------------------------------------------------
    # _printSelections()
    # -------------------------------------------------------------------------
    def _printSelections(self):
        print('Current selection: {}'.format(self.missionInputDict))
        print('Input file: {}'.format(self.filePath.get()))
        print('Output file: {}'.format(self.outputFilePath.get()))
        print('No data {}'.format(self.noData.get()))
        print('Errored data {}'.format(self.erroredData.get()))

    # -------------------------------------------------------------------------
    # addLogWindow()
    #
    # Adds another window which redirects STDOUT to it.
    # -------------------------------------------------------------------------
    def _addLogWindow(self):
        self.addLog = tk.Toplevel(self.master)
        self.addClass = LogWindow(self.master, self.addLog)
        return self.addClass


# -------------------------------------------------------------------------
# LogWindow
#
# Secondary window to show output from program.
# -------------------------------------------------------------------------
class LogWindow(tk.Frame):
    def __init__(self, parent, child):
        tk.Frame.__init__(self, child)
        self.parent = parent
        self.child = child
        self.child.title('NEPAC Data Retriever Log')
        self.child.geometry('800x500')
        self.log = ScrolledTextOut(
            self.child,
            wrap=tk.WORD,
            height=35, width=80, undo=True)
        self.log.grid(column=0, pady=10, padx=10)


# -------------------------------------------------------------------------
# ScrolledTextOut
#
# Obhject to write to LogWindow scrolledtext box
# -------------------------------------------------------------------------
class ScrolledTextOut(scrolledtext.ScrolledText):
    def write(self, s):
        self.insert(tk.CURRENT, s)
        self.see(tk.END)

    def flush(self):
        pass


if __name__ == '__main__':
    window = tk.Tk()
    nepacGUI = NepacProcessGUI(window)
    window.mainloop()

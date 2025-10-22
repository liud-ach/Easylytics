# Easylytics
Tool for performing before/after intervention analysis with Epic Signal Data.

requirements.txt are those for the webapp.py file.

analysis.ipynb is the original jupyter notebook tool that was developed.

Create folders in the same folder as the webapp.py file called 'AMB' for ambulatory exports and 'IP' for inpatient exports and put your signal exports there.  You should then be able to run webapp.py.  

webapp.py was then turned into an executable with pyinstaller.  You will have to perform these steps yourself if looking to create a standalone executable.

INTRODUCTION    
=========================================

LaunchSpace is a set of Python scripts that allow BaseSpace users to automatically launch analysis on their samples as soon as they are ready. These analyses are then tracked to completion, with optional automated quality control and download of a subset of the generated files. The scripts use analysis templates to configure how samples should be analysed. To provide hands-off automation, these tools are designed to run using the Unix service cron to periodically run the scripts and start analysis whenever it is ready.

AUTHORS
=========================================

Peter Saffrey (psaffrey@illumina.com)
Rodger Constandse (rconstandse@illumina.com)


REQUIREMENTS
=========================================

- Python2.7
- A number of Python modules, details in the dependencies.pip file found in the repository
- sqlite3
- The BaseSpace Python SDK:

https://developer.basespace.illumina.com/docs/content/documentation/sdk-samples/python-sdk-overview

INSTALL
=========================================

Ensure your python has all the proper dependencies installed:
    pip install -r dependencies.pip

The scripts are designed to run in place in the location they have been downloaded and unpacked.


GETTING STARTED
=========================================

These instructions assume an appropriate version of Python in $PYTHON and that the code has been checked out into $LAUNCHSPACE. It also assumes an installation of the BaseSpace Python SDK and valid user credentials - for more details see the BaseSpace Python SDK documentation.

Workflow overview
-----------------------------------------

LaunchSpace uses a local configuration database (using sqlite3) to store information about projects and apps, linking these to their corresponding entities in BaseSpace. Command line tools allow the creation of samples (either individually or in batches) within projects where each sample can have one or more linked app. A Launcher tool queries BaseSpace for each sample and project to see whether there is any data in BaseSpace under that sample name. If there is and it meets yield requirements, the associated app is launched on that sample data. Once launched, a BaseSpace AppSessionId is stored locally, linking the sample app run to its associated BaseSpace entity. The Tracker uses this information to track the app to completion. Once completed, the same AppSessionId is used by the QCChecker to download an appropriate metrics file and compare this to a set of thresholds for the relevant app, to mark this analysis as qc-passed or qc-failed. The Downloader then downloads a specified group of files to local storage for delivery or further analysis.

Initialise local configuration database
-----------------------------------------

$PYTHON $LAUNCHSPACE/bin/InitialiseDatabase.py

- This creates a database file in $LAUNCHSPACE/data/db.sqlite
- If you run the command and a database file already exists, the command will exit with an error
- The crontab that ships with LaunchSpace includes a daily backup of the database, which is simply a copy of the database file. In the unlikely event that your database corrupts, you can just copy the most recent backup back into place.


Initialise projects 
-----------------------------------------

Initialising a project makes LaunchSpace aware that a BaseSpace project is of interest for analysis. Project initialisation requires two arguments:

- Project Name (-n)
- Output directory (-p)

### Example:

$PYTHON $LAUNCHSPACE/bin/CreateProject.py -n Project Test -p /projects/Test

- If the specified project is accessible under the provided user credentials, LaunchSpace will obtain the BaseSpace ID for this project and set it into the configuration database.
- If the project does not exist, it will be created and added into the configuration database.
- The output directory is where downloaded data from app output will be stored. The directory needs to exist; an error will be generated if it does not.


Initialise apps
-----------------------------------------

App initialisation requires the following arguments:

- App name (-n)
- Path to a json launch template (-t)
- Path to a json file containing metrics thresholds (-r)
- Metrics file extension (-m)
- App type (either SingleGenome or TumourNormal) (-y)
- Comma-separated list of file extensions that constitute a deliverable (-d)
- BaseSpace ID for the app (-b)

### Example:

$PYTHON $LAUNCHSPACE/bin/CreateApp.py -n IsaacV2 -t data/apptemplates/IsaacV2Template.json -r data/thresholds/isaacv2.json -m summary.csv -y SingleGenome -d vcf,report.pdf -b 278278

- The Launch template is a json file template that will be filled in with appropriate details for a particular sample app launch. Each app requires a different app template. LaunchSpace comes bundled with a set of templates for commonly used apps; these can be found in $LAUNCHSPACE/data/apptemplates
- The metrics file extension and metrics thresholds are used by the QCChecker. The file extension should uniquely identify a file within the AppResult that contains metrics. These will be compared to the thresholds specified in the metrics threshold file provided by the -r extension. 
- Metrics thresholds files have a specified format; for examples, see $LAUNCHSPACE/data/thresholds/
- The deliverable extensions are used by the Downloader to choose which files to download when an app has finished. All files ending with the specified extensions will be downloaded. These extensions can be compound, such as .vcf.gz
- You can find the ID for the app by navigating to it through the BaseSpace website and extracting the ID number from the URL.


Accessioning samples
-----------------------------------------

LaunchSpace can only launch analysis on samples that have been specifically accessioned within its local configuration database. This allows users to select samples they want to analyse and LaunchSpace can then automatically launch apps as soon as data becomes available. Accessioning samples in this way does not alter BaseSpace, only the local configuration database. It makes LaunchSpace aware that data for a particular sample name is expected and that as soon as that data is available an app should be run on it.

Samples are accessioned using the $LAUNCHSPACE/bin/CreateSamples.py command, which has a number of different input mechanisms:

- Accession an individual sample, along with an optional pair (the pair is used for tumour/normal analysis)
- Accession a group of samples via a tsv file with a specified format. This format also supports specifying tumour/normal relationships.
- Accession a group of samples using a Clarity LIMS manifest file. This format also supports specifying tumour/normal relationships.

### SampleApps

As well as accessioning a sample in the local configuration database (which can be queried with ListSamples.py, see below), accessioning a sample alongside its app also creates a SampleApp entry, which LaunchSpace uses to track the progress of the analysis assigned to this sample. Each SampleApp has a status, which is updated as LaunchSpace launches the app and tracks its progress. A sample can have more than one app assigned to it; each SampleApp is tracked separately. SampleApp entries can be tracked with ListSampleApps.py (see below).

### Examples:

Accession a sample called NA12878_Expt18_mpx2_TSNano_704 within the Project Test project which should have the IsaacV2 app run on it:

$PYTHON $LAUNCHSPACE/bin/CreateSamples.py -p Project Test -a IsaacV2 -n NA12878_Expt18_mpx2_TSNano_704

Accession samples from a tsv file included in the LaunchSpace repository:

$PYTHON $LAUNCHSPACE/bin/CreateSamples.py -p Project Test -f $LAUNCHSPACE/data/samplelists/testlist.tsv

Accession samples from a Clarity LIMS manifest included in the LaunchSpace repository:

$PYTHON $LAUNCHSPACE/bin/CreateSamples.py -p Project Test -l $LAUNCHSPACE/data/samplelists/ClarityLIMSSampleList.txt

- The project name specified must already have been created within the local configuration database with CreateProject.py
- The app name specified (either in the single sample or file inputs) must already have been created within the local configuration database with CreateApp.py

Run the Launcher
-----------------------------------------

The Launcher is the tool that launches BaseSpace apps across all SampleApp entries that meet the proper conditions. It executes the following set of steps:

- Look up all the SampleApp entries with the status of waiting. These are samples that have only just been created or the last time the launcher was run there was no data for this sample, or the available data did not meet the yield requirements
- For each of these SampleApps:
    - Query the associated BaseSpace project to find the available samples and see if any data is available for this sample (NB. LaunchSpace only queries once to get all samples for each project for each Launcher run and caches the results, to minimise API chatter)
    - If data is available, check whether the yield matches the specified limits.
    - If it does, fill in the appropriate app template and submit it to BaseSpace to launch the app. Capture the AppSession ID that is returned by BaseSpace. This will be used by the Tracker to track the app.
    - If launched, set the status of the SampleApp entry to be submitted. If anything goes wrong, set the status to launch-failed

The Launcher is designed to be run without arguments as a cron entry (see below). In this mode, it runs with no output to stdout or stderr, outputting any messages into a log file. However, it can be useful to run the Launcher manually to check what would happen or if manual intervention is needed. In these cases, the following options are useful:

- Run on only one SampleApp (-i) (provide the SampleApp ID and only attempt to launch this one. For working with problem entries)
- Safe mode (-s) (output what would the Launcher would do without actually doing it. This could also be thought of as a dry run mode.)
- Output to stdout (-l) (when running manually, output to stdout instead of to the default log file)
- Increase level of logging (-L DEBUG) (usually used in combination with -l to see more detail about what the Launcher is doing)
- Ignore low yield (-Y) (launch app so long as data exists
 even if it does not meet yield requirements. Particularly useful in combination with -i to force launch of a sample that is near the yield requirements)

### Examples:

Show in detail what you would do without doing it:

$PYTHON $LAUNCHSPACE/bin/Launcher.py -s -L DEBUG -l

Launch app on SampleApp entry with id 14, even if there is not enough yield. Show output to stdout:

$PYTHON $LAUNCHSPACE/bin/Launcher.py -i 14 -Y -l

Run the Tracker
-----------------------------------------

The Tracker is the tool that tracks submitted and running SampleApp entries updating their status. It executes the following set of steps:

- Look up all the SampleApp entries with the status of submitted, pending or running
- For each of these SampleApps:
    - Lookup the BaseSpace AppSession ID acquired when the app was launched.
    - Ask BaseSpace for the status of this app
    - Set the status against this SampleApp. The new status should be pending, running, app-finished or run-failed

Like the Launcher the Tracker is designed to be run on a cron and only provide output into a log file. Also like the Launcher there are arguments for manual intervention:

- Run on only one SampleApp (-i) (provide the SampleApp ID and only attempt to update this one)
- Safe mode (-s) (output what would the Tracker would do without actually doing it)
- Output to stdout (-l) (when running manually, output to stdout instead of to the default log file)
- Increase level of logging (-L DEBUG) (usually used in combination with -l to see more detail about what the Tracker is doing)

Examples:

Show in detail what you would do without doing it:

$PYTHON $LAUNCHSPACE/bin/Tracker.py -s -L DEBUG -l

Run the QCChecker
-----------------------------------------

The QCChecker is the tool that pulls down a specific metrics file from an app result and evaluates whether those metrics are within specified thresholds. It goes through the following steps:

- Look up all the SampleApp entries with the status of app-finished
- For each of these SampleApps:
	- Download and parse the specified metrics file to extract metrics
	- Compare these metrics to the thresholds for the app
	- Assign a status of qc-passed or qc-failed

The QCChecker has the same manual options as the Tracker - individual SampleApps, safe mode and debugging output.


Run the Downloader
-----------------------------------------

The Downloader is the tool that pulls down the "deliverable" for an app result - all the files with any of a set of extensions. It goes through the following steps:

- Look up all the SampleApp entries with the status of qc-passed
- For each of these SampleApps:
	- Make an output directory for the SampleApp based on the project output directory
	- For each of the deliverable extensions, find and download any matching files

The Downloader has the same manual options as the Tracker - individual SampleApps, safe mode and debugging output.

AUTOMATING THE WORKFLOW
=========================================

Installing a crontab
-----------------------------------------

It is recommended that before the LaunchSpace scripts are installed under a crontab, each of the cron tools (Launcher, Tracker, QCChecker and Downloader) are run manually for a few samples to understand their operation. This will help provide a smooth integration of the scripts.

A crontab is included in the root of the LaunchSpace repository. It includes all the entries necessary to have a fully automated system, where all the user needs to do is accession samples and point their sequencing devices at BaseSpace and everything else should happen without intervention. In practice, some proportion of samples always need manual intervention; these are described in more detail below.

Before the crontab can be installed, it should be edited to include the path to the Python executable and LaunchSpace source code. The crontab can then be installed up by running:

crontab $LAUNCHSPACE/crontab

checked by running:

crontab -l

and edited by running:

crontab -e

Note that cron runs as a specific user, and this user must have the proper BaseSpace credentials setup in their .basespacepy.cfg file.

Monitoring progress
-----------------------------------------

The happy path for samples handled by LaunchSpace should be as follows:

- Sample accessioned
- Sequencing data arrives in BaseSpace
- SampleApp launched by Launcher
- SampleApp tracked by Tracker until it reaches app-finished state
- Metrics downloaded by QCChecker and this result found to pass - SampleApp marked as qc-passed
- Deliverable downloaded by Downloader - SampleApp marked as downloaded

During each of these stages, there are several ways to keep track of the SampleApp run. While the app is running, within BaseSpace itself you can find the AppSession for the SampleApp in the UI and monitor progress there, including checking any logging output.

For bulk checking and to track the status after the app run has finished, LaunchSpace also provides the tool ListSampleApps.py to query the status of SampleApps and, if necessary manually adjust their status. It has the following options:

Get SampleApp entries by:
- Individual SampleApp ID (-i)
- Specific app name (-n)
- Project (-p)
- Sample name (-s)
- SampleApp status (-u)
- App type (-y)

These options use substring matching by default. Exact matching can be switched on by using -x

You can report the status details field of the SampleApp entry by adding a -e. These details might include the reason a SampleApp is waiting (for example No data or Not enough yield)
 why a sample failed QC or the error message provided when an app failed to download.

Finally
 you can also opt to apply an operation to all the SampleApps selected by the other arguments:

- Delete (-D)
- Manually set sample status (-S <newstatus>)
 
The typical pattern for applying these options would be to first write and test a set of options to get the SampleApps of interest. Then add the -S to set their status.

For example
 get all the SampleApps from a particular project with status qc-failed:

$PYTHON $LAUNCHSPACE/bin/ListSampleApps.py -p Project Test -u qc-failed

$PYTHON $LAUNCHSPACE/bin/ListSampleApps.py -p Project Test -u qc-failed -S qc-passed

 
Intervening in problem samples
-----------------------------------------

Below is a list of possible deviations from the happy path and actions that can be taken to correct this. In many cases, these suggestions involve using ListSampleApps.py to find the problem cases and manually set their status to move them on or force them to repeat a step.

### Data for sample is not found in BaseSpace

- Check the project name is correct and accessioned properly in the local configuration database
- Check the user credentials (access token) available to LaunchSpace can read the proper project

### SampleApp is marked as launch-failed

- Check the app template is correct and matches the BaseSpace app ID.
- Check the user credentials (access token) avialable to LaunchSpace has permissions within BaseSpace to launch the chosen app

### SampleApp is marked as run-failed

- Check the AppSession through the BaseSpace web page to see if the problem can be debugged.
- Try relaunching the SampleApp from scratch by setting the status back to waiting

### SampleApp is marked as qc-failed

- Choose to manually override this decision and mark the SampleApp as qc-passed
- Do further sequencing to produce new data. Set the SampleApp as waiting and this latest data will be picked up for a repeat run

### SampleApp is marked as download-failed

- This may be a temporary problem such as a network outage. Try marking the sample as qc-passed and the download will be retried.
- You can also try running the Downloader in safe mode to get the individual command to download the results from one SampleApp. Then run this manually to debug any problems.

OTHER MONITORING TOOLS
=========================================

Tool | Purpose
ListProjects.py | List accessioned projects
ListSamples.py | List accessioned samples with their associated project name
ListApps.py | List details of all the accessioned apps

FURTHER NOTES AND KNOWN LIMITATIONS
=========================================

QC metrics business logic
-----------------------------------------

The logic to unpack the metrics for an app has been tested against the Isaac V2 and tumour/normal apps. For other apps this might need to be extended or modified to extract the metrics properly. This logic is found in AppServices.py in the _ReadQCResult() method.

Project Sample Limit
-----------------------------------------

For projects with greater than 1000 items, the BaseSpace API requires the use of paginated requests that are not supported by LaunchSpace. Therefore, If your project contains more than 1000 samples LaunchSpace will not function properly. Extending LaunchSpace to support pagination would be possible in a future release. As a workaround, we recommend creating separate projects in these cases - this can be done by service period, for example myproject_jan15, myproject_feb15 or myproject_q1, myproject_q2. Separate projects can also be more convenient for large numbers of samples.

Direct Database Manipulation
-----------------------------------------

LaunchSpace is based on sqlite and the database created has triggers and foreign keys set up to implement business logic about updating columns and keeping tables consistent. For example, if you delete a Sample, it will also delete any associated SampleApp entries. This means you should be able to use standard sqlite tools to work with the database (found by default in $LAUNCHSPACE/data/db.sqlite) without interfering with the operation of the LaunchSpace scripts. This might be desirable if you need to perform operations that are not directly supported by the scripts or for other customisation.

During LaunchSpace development, we used sqlitebrowser:

http://sqlitebrowser.org/

as well as the command line tool sqlite3 to inspect and alter the database. 

The only caveat with these tools is that you need to ensure the PRAGMA for foreign keys is switched on or these will not be properly enforced. In sqlitebrowser this can be set in the "Edit Pragmas" tab. For the command line tool, create a .sqliterc file in your home directory containing this line:

PRAGMA foreign_keys=1;


GLOSSARY
=========================================

SampleApp status table:

Status | Description
waiting | Sample has been accessioned but not yet checked by Launcher OR sample has been checked but does not yet meet launch conditions
submitted | App has been submitted to BaseSpace
pending | App run is pending execution in BaseSpace
running | App is running in BaseSpace
launch-failed | App failed to launch. The error message can be seen with ListSampleApps.py -e
run-failed | App failed whilst running
app-finished | The app finished successfully
qc-failed | The app result failed QC. Some details on the failure can be seen with ListSampleApps.py -e
qc-passed | The app result passed QC
downloading | The app result deliverable is currently being downloaded
download-failed | The app result failed while downloading. Some details on the failure can be seen with ListSampleApps.py -e
downloaded | The app result has downloaded

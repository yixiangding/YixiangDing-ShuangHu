if [ -z "$1" ]
	then
	echo "Application's name is missing (#1)"
	exit 0
fi
application="$1"

if [ -z "$2" ]
	then
	echo "Analysis's name is missing (#2)"
	exit 0
fi
analysisName="$2"

if [ -z "$3" ]
	then
	echo "Recovery Method is missing (#3)"
	exit 0
fi


recoveryMethodName="$3"
unusefulFile="META-INF"

fE1="ser"
fE3="deps.rsf"

if [[ "$recoveryMethodName" = "BCE" ]]
then
	fE2="clusters.rsf"
elif [[ "$recoveryMethodName" = "BatchPackager" ]] 
then
	fE2="pkgs.rsf"
elif [[ "$recoveryMethodName" = "AWSD_SEA" ]] 
then
	fE2="clustered.rsf"
else 
	echo "Recovery Method Name should be BCE, AWSD_SEA, or BP"
	exit
fi

echo $fE2

arcadeFolder="/Users/pooyan/git/arcade"
workingFolder="$arcadeFolder/pooyan"
recoverResultFolder="$workingFolder/$recoveryMethodName/$application"

AnalyseApplicationFolder="$workingFolder/$analysisName/$application/$recoveryMethodName"
rm -r $AnalyseApplicationFolder
mkdir -p $AnalyseApplicationFolder

executionOutput="$AnalyseApplicationFolder/$application-$recoveryMethodName-$analysisName.out"
rootLogSrc="$arcadeFolder/logs/root.log"
rootLogDst="$AnalyseApplicationFolder/$application-$recoveryMethodName-$analysisName.log"

smellSerFilesCollection="$AnalyseApplicationFolder/smellSer"
mkdir $smellSerFilesCollection
rm $smellSerFilesCollection/*
./script_fileCollector.sh $fE1 $recoverResultFolder $smellSerFilesCollection

clusteredRsfFilesCollection="$AnalyseApplicationFolder/clusterRsf"
mkdir $clusteredRsfFilesCollection
rm $clusteredRsfFilesCollection/*
./script_fileCollector.sh $fE2 $recoverResultFolder $clusteredRsfFilesCollection

depsRsfFilesCollection="$AnalyseApplicationFolder/depsRsf"
mkdir $depsRsfFilesCollection
rm $depsRsfFilesCollection/*
./script_fileCollector.sh $fE3 $recoverResultFolder $depsRsfFilesCollection

./script_cleaner.sh $AnalyseApplicationFolder "$unusefulFile"

echo "running analysis : $analysisName "
./script_exec_$analysisName.sh  $smellSerFilesCollection $clusteredRsfFilesCollection $depsRsfFilesCollection $executionOutput
echo "The excecution outPut is saved at $executionOutput"

cp  $rootLogSrc $rootLogDst
echo "log file is at $rootLogDst"

exit 0

 
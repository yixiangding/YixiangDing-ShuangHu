if [ -z "$1" ]
	then
	echo "RecoveryMethod's name is missing (#1)"
	exit 0
fi

if [ -z "$2" ]
	then
	echo "Analyses Folder is missing (#2), this is the folder where the results of a particular analyses are"
	exit 0
fi

recoveryMethodName=$1
analysesFolder=$2

if [[ "$recoveryMethodName" = "ARC" ]]
then
	recoveryMethodName="BCE"
elif [[ "$recoveryMethodName" = "ACDC" ]]
then
	recoveryMethodName="AWSD_SEA"
elif [[ "$recoveryMethodName" = "BP" ]]
then
	recoveryMethodName="BatchPackager"
fi

echo "recovery method is : $recoveryMethodName"


for i in $(ls  "$analysesFolder"); do
	#$i is the folder containing analyses on a portion of versions of an application
	#echo $i $recoveryMethodName
	./script_generic.sh $i BSE $recoveryMethodName
done
exit 0
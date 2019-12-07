if [ -z "$1" ]
	then
	echo "Application's name is missing (#1)"
	exit 0
fi
if [ -z "$2" ]
	then
	echo "Analysis's name is missing (#2)"
	exit 0
fi

recoveryMethodName=$2

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
echo $recoveryMethodName



./script_generic.sh $1 MEA $recoveryMethodName
./script_STIC.sh $1 $recoveryMethodName
./script_generic.sh $1 C2C $recoveryMethodName
./script_generic.sh $1 BDMA $recoveryMethodName
./script_generic.sh $1 SDA $recoveryMethodName
./script_generic.sh $1 BSE $recoveryMethodName
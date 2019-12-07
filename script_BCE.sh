source script_config.sh

if [ -z "$1" ]
then
	echo "Application's name (#1) is missing"
	exit 0 
fi
if [ -z "$2" ]
then
	echo "Application's version (#2) is missing"
	exit 0
fi
if [ -z "$3" ]
then
	echo "binary root folder (#3) is missing"
	exit 0
fi

binaryRootFolder="$3"

#Date is added! it should be removed!
#Date=$(date 'x+%Y-%m-%d-%H-%M-%S')
application="$1"
version="$2"


AnalysesFolder="$RootFolder/BCE"

AnalAppVersionFolder="$AnalysesFolder/$application/$version"

rm -r $AnalAppVersionFolder
output_dir_1="$AnalAppVersionFolder/artifact"
mkdir -p $output_dir_1

BinaryAppVersionFolder="$binaryRootFolder/$application/$version/"

echo "running BatchClusteringEngine..."

echo "$BinaryAppVersionFolder $output_dir_1 build src"

/Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home/bin/java -Dfile.encoding=US-ASCII -classpath /Users/pooyan/git/arcade/bin:/Users/pooyan/git/arcade/libs/weka.jar:/Users/pooyan/git/arcade/libs/jxl.jar:/Users/pooyan/git/arcade/libs/log4j-1.2.16.jar:/Users/pooyan/git/arcade/libs/guava-10.0.1.jar:/Users/pooyan/git/arcade/libs/commons-cli-1.2.jar:/Users/pooyan/git/arcade/libs/sootclasses-2.4.0.jar:/Users/pooyan/git/arcade/libs/polyglotclasses-1.3.5.jar:/Users/pooyan/git/arcade/libs/jasminclasses-2.4.0.jar:/Users/pooyan/git/arcade/libs/gexf4j-core-0.2.0-ALPHA.jar:/Users/pooyan/git/arcade/libs/Jama-1.0.2.jar:/Users/pooyan/git/arcade/libs/jgrapht-jdk1.6.jar:/Users/pooyan/git/arcade/libs/collections-generic-4.01.jar:/Users/pooyan/git/arcade/libs/colt-1.2.0.jar:/Users/pooyan/git/arcade/libs/vecmath-1.3.1.jar:/Users/pooyan/git/arcade/libs/j3d-core-1.3.1.jar:/Users/pooyan/git/arcade/libs/jung-api-2.0.1.jar:/Users/pooyan/git/arcade/libs/concurrent-1.3.4.jar:/Users/pooyan/git/arcade/libs/jung-algorithms-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-visualization-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-3d-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-graph-impl-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-3d-demos-2.0.1.jar:/Users/pooyan/git/arcade/libs/wstx-asl-3.2.6.jar:/Users/pooyan/git/arcade/libs/stax-api-1.0.1.jar:/Users/pooyan/git/arcade/libs/jung-io-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-jai-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-samples-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-jai-samples-2.0.1.jar:/Users/pooyan/git/arcade/libs/poi-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-ooxml-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/xmlbeans-2.3.0.jar:/Users/pooyan/git/arcade/libs/dom4j-1.6.1.jar:/Users/pooyan/git/arcade/libs/junit-3.8.1.jar:/Users/pooyan/git/arcade/libs/poi-ooxml-schemas-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-excelant-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-examples-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-scratchpad-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/commons-math3-3.0.jar:/Users/pooyan/git/arcade/libs/commons-lang3-3.1.jar:/Users/pooyan/git/arcade/libs/snowball.jar:/Users/pooyan/git/arcade/libs/xstream-1.4.6.jar:/Users/pooyan/git/arcade/libs/xstream/xmlpull-1.1.3.1.jar:/Users/pooyan/git/arcade/libs/xstream/xpp3_min-1.1.4c.jar:/Users/pooyan/git/arcade/libs/classcycle.jar:/Users/pooyan/git/arcade/libs/trove-2.0.2.jar:/Users/pooyan/git/arcade/libs/jira-client-0.5-SNAPSHOT-jar-with-dependencies.jar:/Users/pooyan/git/arcade/libs/svnkit-1.8.3.jar:/Users/pooyan/git/arcade/libs/jcommander-1.36-SNAPSHOT.jar:/Users/pooyan/git/arcade/libs/commons-io-2.4.jar:/Users/pooyan/git/arcade/libs/mallet.jar:/Applications/eclipse/plugins/org.junit_4.10.0.v4_10_0_v20120426-0900/junit.jar:/Applications/eclipse/plugins/org.hamcrest.core_1.1.0.v20090501071000.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.contenttype_3.4.200.v20130326-1255.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.resources_3.8.101.v20130717-0806.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.runtime_3.9.0.v20130326-1255.jar:/Users/pooyan/git/arcade/libs/org.eclipse.jdt.core_3.9.1.v20130905-0837.jar:/Users/pooyan/git/arcade/libs/org.eclipse.osgi_3.9.1.v20130814-1242.jar:/Users/pooyan/git/arcade/libs/org.eclipse.equinox.common_3.6.200.v20130402-1505.jar:/Users/pooyan/git/arcade/libs/org.eclipse.equinox.preferences_3.5.100.v20130422-1538.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.jobs_3.5.300.v20130429-1813.jar edu.usc.softarch.arcade.clustering.BatchClusteringEngine $BinaryAppVersionFolder $output_dir_1 build src
#/Users/pooyan/git/arcade/pooyan/binaryFiles/ivy /Users/pooyan/git/arcade/pooyan/BCE/ivy build src

echo "done"
exit

/Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home/bin/java -Dfile.encoding=US-ASCII -classpath /Users/pooyan/git/arcade/bin:/Users/pooyan/git/arcade/libs/weka.jar:/Users/pooyan/git/arcade/libs/jxl.jar:/Users/pooyan/git/arcade/libs/log4j-1.2.16.jar:/Users/pooyan/git/arcade/libs/guava-10.0.1.jar:/Users/pooyan/git/arcade/libs/commons-cli-1.2.jar:/Users/pooyan/git/arcade/libs/sootclasses-2.4.0.jar:/Users/pooyan/git/arcade/libs/polyglotclasses-1.3.5.jar:/Users/pooyan/git/arcade/libs/jasminclasses-2.4.0.jar:/Users/pooyan/git/arcade/libs/mallet.jar:/Users/pooyan/git/arcade/libs/gexf4j-core-0.2.0-ALPHA.jar:/Users/pooyan/git/arcade/libs/Jama-1.0.2.jar:/Users/pooyan/git/arcade/libs/jgrapht-jdk1.6.jar:/Users/pooyan/git/arcade/libs/collections-generic-4.01.jar:/Users/pooyan/git/arcade/libs/colt-1.2.0.jar:/Users/pooyan/git/arcade/libs/vecmath-1.3.1.jar:/Users/pooyan/git/arcade/libs/j3d-core-1.3.1.jar:/Users/pooyan/git/arcade/libs/jung-api-2.0.1.jar:/Users/pooyan/git/arcade/libs/concurrent-1.3.4.jar:/Users/pooyan/git/arcade/libs/jung-algorithms-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-visualization-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-3d-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-graph-impl-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-3d-demos-2.0.1.jar:/Users/pooyan/git/arcade/libs/wstx-asl-3.2.6.jar:/Users/pooyan/git/arcade/libs/stax-api-1.0.1.jar:/Users/pooyan/git/arcade/libs/jung-io-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-jai-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-samples-2.0.1.jar:/Users/pooyan/git/arcade/libs/jung-jai-samples-2.0.1.jar:/Users/pooyan/git/arcade/libs/poi-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-ooxml-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/xmlbeans-2.3.0.jar:/Users/pooyan/git/arcade/libs/dom4j-1.6.1.jar:/Users/pooyan/git/arcade/libs/junit-3.8.1.jar:/Users/pooyan/git/arcade/libs/poi-ooxml-schemas-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-excelant-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-examples-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/poi-scratchpad-3.8-20120326.jar:/Users/pooyan/git/arcade/libs/commons-math3-3.0.jar:/Users/pooyan/git/arcade/libs/commons-lang3-3.1.jar:/Users/pooyan/git/arcade/libs/snowball.jar:/Users/pooyan/git/arcade/libs/xstream-1.4.6.jar:/Users/pooyan/git/arcade/libs/xstream/xmlpull-1.1.3.1.jar:/Users/pooyan/git/arcade/libs/xstream/xpp3_min-1.1.4c.jar:/Users/pooyan/git/arcade/libs/classcycle.jar:/Users/pooyan/git/arcade/libs/trove-2.0.2.jar:/Users/pooyan/git/arcade/libs/jira-client-0.5-SNAPSHOT-jar-with-dependencies.jar:/Users/pooyan/git/arcade/libs/svnkit-1.8.3.jar:/Applications/eclipse/plugins/org.junit_4.10.0.v4_10_0_v20120426-0900/junit.jar:/Applications/eclipse/plugins/org.hamcrest.core_1.1.0.v20090501071000.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.contenttype_3.4.200.v20130326-1255.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.resources_3.8.101.v20130717-0806.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.runtime_3.9.0.v20130326-1255.jar:/Users/pooyan/git/arcade/libs/org.eclipse.jdt.core_3.9.1.v20130905-0837.jar:/Users/pooyan/git/arcade/libs/org.eclipse.osgi_3.9.1.v20130814-1242.jar:/Users/pooyan/git/arcade/libs/org.eclipse.equinox.common_3.6.200.v20130402-1505.jar:/Users/pooyan/git/arcade/libs/org.eclipse.equinox.preferences_3.5.100.v20130422-1538.jar:/Users/pooyan/git/arcade/libs/org.eclipse.core.jobs_3.5.300.v20130429-1813.jar edu.usc.softarch.arcade.AcdcWithSmellDetection $BinaryAppVersionFolder $output_dir_1/ .

cp logs/root.log $output_dir_1

exit 

 
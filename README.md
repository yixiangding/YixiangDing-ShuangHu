# CS578 ReadMe
Team members: 
- Shuang Hu
- Yixiang Ding

## Description
The current implementation of ARC are reading source code line by line but there's no analysis being carried out on security issues. However there do exist some patterns that the vulnerabilities could be detected through scanning the source code. For example there could be some WebSocket vulnerability that not enough secured techniques being applied.
### Target vulnerability
Tomcat [CVE-2018-8034](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2018-8034)

Discover potential vulnerability. The modifications are based on ARC analysis.


## Usage
### Subject System
Tomcat 7
### Requirements
> Java 8
> IntelliJ IDEA 2017.2.2
> [CS578-Arcade](https://github.com/asejfia/CS578-arcade)
> Vulnerability Analyzer by this project
### IDE Configuration
> Main Class: edu.usc.softarch.arcade.util.ldasupport.PipeExtractor
> Program arguments: subject_systems/tomcat subject_systems/output/arc/base
### Usage
1. use intellij to run with above configuration
> Main Class: edu.usc.softarch.arcade.util.ldasupport.PipeExtractor
> Program arguments: subject_systems/tomcat subject_systems/output/arc/base
3. generate mallet file with below command
```
./ext-tools/mallet-2.0.7/bin/mallet import-dir --input subject_systems/<target_project_src>/ --remove-stopwords TRUE --keep-sequence TRUE --output <output_dir/topicmodel.data>
```
3. generate inference with below command
```
./ext-tools/mallet-2.0.7/bin/mallet train-topics --input <output_dir/topicmodel.data> --inferencer-filename <output_dir/infer.mallet> --num-top-words 50 --num-topics 100 --num-threads 3 --num-iteration 100 --doc-topics-threshold 0.1
```

## Expected Result
### Resulting files
```
subject_systems/<target_project>/output/arc/base/
|---output.pipe
│---potential_vul.csv 
```
### Original Version
potential_vul.csv with a list of affected files using webSocket and corresponding vulnerability status(yes/no).
```
...
TestWsRemoteEndpoint.java,false
TestWsWebSocketContainerWithProxy.java,false
WsWebSocketContainer.java,true
...
```
### Fixed Version
potential_vul.csv with no vulnerabilities found.
```
...
TestWsRemoteEndpoint.java,false
TestWsWebSocketContainerWithProxy.java,false
WsWebSocketContainer.java,false
...
```

## Algorithm
A list of non-testing files related to WebSocket are spotted first which are meant to follow certain security essential rules in secure architecture design. Source files under SSL related architecture that is not followed by those rules contributes security related vulnerabilities.
To ensure the suject system has secured architecture, hereby a policy based algorithm for WebSocket vulnerability has been implemented. The policies that have been applied to this prototype includes:
1. Ensured endpoint has used a secured algorithm (e.g. HTTPS).
2. Finding security configuration (e.g. SSL config) and analyse potential vulnerabilies.

## Changes
> src/edu/usc/softarch/arcade/util/WsVulnerabilityAnalyser/Filters.java
> src/edu/usc/softarch/arcade/util/WsVulnerabilityAnalyser/Policies.java
> src/edu/usc/softarch/arcade/util/WsVulnerabilityAnalyser/Results.java
> src/edu/usc/softarch/arcade/util/WsVulnerabilityAnalyser/VulnerabilityAnalyser.java
> src/edu/usc/softarch/arcade/util/ldasupport/PipeExtractor.java


## Future Work
1. Specify the type of certain vulnerability in addition to pointing out the existence of vulnerabilities.
2. Include more features in vulnerability analyzer apart from websocket vulnerability.


## Answers to questions in spec
- Task1: ARC is selected. It uses words in project source code and machine learning techniques to do architecture recovery. Subject System: Tomcat 7
- Task2: Current implementation of ARC doesn't include security decision because requires a lot of domain knowledges to determine which component is related to 'Security' and which component is related to other functionalities. Certain mechanism will have to be applied to introduce domain knowledges.
- Task3: Specified in Changes session
- Task4: By highlight related source files related to websocket which contains security decisions(whether or not to use secure layer architecture). The affected elements are all source files listed.
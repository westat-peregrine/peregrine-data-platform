# Peregrine Data Platform
The Peregrine Data Platform is an AWS CDK application written in Java that deploys a data analytics pipeline using S3, Glue, and Athena.  As new data are uploaded to the Peregrine S3 bucket, a Glue crawler is triggered to determine the schema and make the data available for querying using Athena.  Sample JSON and CSV datasets are included.  
<br>

![Peregrine Diagram](diagram.png)

## Requirements

The following requirements must be met prior to deployment:

1. [AWS CDK Prerequisites](https://docs.aws.amazon.com/cdk/latest/guide/work-with.html#work-with-prerequisites)
2. [Java Prerequisites](https://docs.aws.amazon.com/cdk/latest/guide/work-with-cdk-java.html#java-prerequisites)
3. [Git](https://git-scm.com/downloads)

## Installation
Once all requirements are met, proceed with cloning and deploying the application.  Be sure to insert appropriate parameter values in the deploy step.


    git clone https://gitlab.westat.com/ausborn_s/peregrine
    cd peregrine-master
    cdk synth
    cdk deploy --parameters email= --parameters sftp= --parameters quicksight=

## Usage
1. Confirm SNS subscription 
2. Retrieve user API key
3. Connect to S3 using Cyberduck or WinSCP
4. Connect to Athena using DBeaver
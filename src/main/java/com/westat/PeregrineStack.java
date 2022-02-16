package com.westat;

import java.util.Arrays;
import java.util.Map;

import software.constructs.Construct;
import software.amazon.awscdk.*;
import software.amazon.awscdk.services.athena.CfnWorkGroup;
import software.amazon.awscdk.services.athena.CfnWorkGroup.EncryptionConfigurationProperty;
import software.amazon.awscdk.services.athena.CfnWorkGroup.ResultConfigurationProperty;
import software.amazon.awscdk.services.athena.CfnWorkGroup.WorkGroupConfigurationProperty;
import software.amazon.awscdk.services.glue.CfnCrawler;
import software.amazon.awscdk.services.glue.CfnCrawler.RecrawlPolicyProperty;
import software.amazon.awscdk.services.glue.CfnCrawler.S3TargetProperty;
import software.amazon.awscdk.services.glue.CfnCrawler.SchemaChangePolicyProperty;
import software.amazon.awscdk.services.glue.CfnCrawler.TargetsProperty;
import software.amazon.awscdk.services.glue.alpha.*;
import software.amazon.awscdk.services.iam.CfnAccessKey;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.iam.User;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.eventsources.SnsEventSource;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.EventType;
import software.amazon.awscdk.services.s3.deployment.BucketDeployment;
import software.amazon.awscdk.services.s3.deployment.Source;
import software.amazon.awscdk.services.s3.notifications.SnsDestination;
import software.amazon.awscdk.services.secretsmanager.CfnSecret;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.EmailSubscription;

public class PeregrineStack extends Stack {
    public PeregrineStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public PeregrineStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // Create parameter for receiving email notifications
        CfnParameter emailAddress = CfnParameter.Builder.create(this, "emailAddress")
            .type("String")
            .description("The email address where upload notifications will be sent")
            .build();

        
        // Create IAM user and API key for S3 + Athena access
        User peregrineUser = User.Builder.create(this, "peregrineUser")
            .userName("falcon")
            .managedPolicies(Arrays.asList(
                ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess"),
                ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSQuicksightAthenaAccess")))
            .path("/peregrine/")
            .build();

        CfnAccessKey peregrineKey = CfnAccessKey.Builder.create(this, "peregrineKey")
            .userName(peregrineUser.getUserName())
            .status("Active")
            .build();

        // Store API key in Secrets Manager
        CfnSecret.Builder.create(this, "peregrineSecret")
            .description("Contains the API key for the Peregrine demo user")
            .name("peregrineKey")
            .secretString("{\"AccessKey\":\""+peregrineKey.getRef()+"\",\"SecretKey\":\""+peregrineKey.getAttrSecretAccessKey()+"\"}")
            .build();

        
        // Create S3 bucket for data
        Bucket peregrineBucket = Bucket.Builder.create(this, "peregrineBucket")
            .bucketName("peregrine-"+this.getAccount())
            .encryption(BucketEncryption.S3_MANAGED)
            .removalPolicy(RemovalPolicy.DESTROY) 
            .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
            .build();

        // Create IAM resources for S3 access
        PolicyStatement listPeregrineBucket = new PolicyStatement();
        listPeregrineBucket.setSid("peregrineBucketList");
        listPeregrineBucket.setEffect(Effect.ALLOW);
        listPeregrineBucket.addActions("s3:ListBucket");
        listPeregrineBucket.addResources(peregrineBucket.getBucketArn());

        PolicyStatement modifyPeregrineData = new PolicyStatement();
        modifyPeregrineData.setSid("peregrineObjectAccess");
        modifyPeregrineData.setEffect(Effect.ALLOW);
        modifyPeregrineData.addActions("s3:PutObject","s3:GetObject","s3:DeleteObject");
        modifyPeregrineData.addResources(peregrineBucket.getBucketArn()+"/*");

        PolicyDocument s3PolicyDocument = new PolicyDocument();
        s3PolicyDocument.addStatements(listPeregrineBucket, modifyPeregrineData);

        // Grant S3 rights to IAM user
        peregrineUser.addToPolicy(listPeregrineBucket);
        peregrineUser.addToPolicy(modifyPeregrineData);


        // Create S3 bucket for Athena
        Bucket athenaBucket = Bucket.Builder.create(this, "athenaBucket")
            .bucketName("aws-athena-query-results-peregrine-"+this.getAccount()) // Bucket name should align with managed policy
            .encryption(BucketEncryption.S3_MANAGED)
            .removalPolicy(RemovalPolicy.DESTROY)
            .autoDeleteObjects(true)
            .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
            .build();

        // Define Athena workgroup
        CfnWorkGroup.Builder.create(this, "athenaWorkGroup")
            .name("peregrine")
            .workGroupConfiguration(WorkGroupConfigurationProperty.builder()
                .enforceWorkGroupConfiguration(true)
                .publishCloudWatchMetricsEnabled(true)
                .resultConfiguration(ResultConfigurationProperty.builder()
                    .encryptionConfiguration(EncryptionConfigurationProperty.builder().encryptionOption("SSE_S3").build())
                    .outputLocation(athenaBucket.s3UrlForObject())
                    .build())
                .bytesScannedCutoffPerQuery(1000000000)
                .build())
            .recursiveDeleteOption(true)
            .build();


        // Create Glue resources for access to data via Athena
        Role peregrineGlueRole = Role.Builder.create(this, "peregrineGlueRole")
            .assumedBy(new ServicePrincipal("glue.amazonaws.com"))
            .roleName("peregrineGlueRole")
            .description("Allows Glue to access objects stored in the Peregrine bucket")
            .inlinePolicies(Map.of("s3GluePolicy",s3PolicyDocument))
            .managedPolicies(Arrays.asList(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")))
            .build();

        Database peregrineDatabase = Database.Builder.create(this,"peregrineDatabase")
            .databaseName("peregrine")
            .build();

        CfnCrawler peregrineCrawler = CfnCrawler.Builder.create(this, "peregrineCrawler")
            .databaseName(peregrineDatabase.getDatabaseName())
            .targets(TargetsProperty.builder().s3Targets(Arrays.asList(S3TargetProperty.builder().path("s3://"+peregrineBucket.getBucketName()).build())).build())
            .role(peregrineGlueRole.getRoleArn())
            .tablePrefix("peregrine_")
            .name("peregrineCrawler")
            .recrawlPolicy(RecrawlPolicyProperty.builder().recrawlBehavior("CRAWL_EVERYTHING").build())
            .configuration("{\"Version\":1,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"}}}")
            .schemaChangePolicy(SchemaChangePolicyProperty.builder().updateBehavior("UPDATE_IN_DATABASE").deleteBehavior("DELETE_FROM_DATABASE").build())
            .build();


        // Create SNS topic for data uploads
        Key s3TopicKey = Key.Builder.create(this, "s3TopicKey")
            .alias("peregrine/s3TopicKey")
            .enableKeyRotation(true)
            .enabled(true)
            .build();

        Topic s3Topic = Topic.Builder.create(this, "s3Topic")
            .displayName("Peregrine Upload Notification")
            .masterKey(s3TopicKey)
            .build();

        s3TopicKey.grantDecrypt(new ServicePrincipal("s3.amazonaws.com").withConditions(Map.of("ForAnyValue:StringEquals", Map.of("kms:EncryptionContextKeys","aws:sns:topicArn"))));
        s3TopicKey.addToResourcePolicy(PolicyStatement.Builder.create()
            .actions(Arrays.asList("kms:GenerateDataKey*"))
            .effect(Effect.ALLOW)
            .principals(Arrays.asList(new ServicePrincipal("s3.amazonaws.com")))
            .conditions(Map.of("ForAnyValue:StringEquals", Map.of("kms:EncryptionContextKeys","aws:sns:topicArn")))
            .resources(Arrays.asList("*"))
            .build());
            
        peregrineBucket.addEventNotification(EventType.OBJECT_CREATED, new SnsDestination(s3Topic));

        
        // Create lambda to execute glue crawler on S3 uploads
        Function crawlerLambda = Function.Builder.create(this, "crawlerLambda")
            .runtime(Runtime.PYTHON_3_9)
            .code(Code.fromAsset("lambda"))
            .handler("runPeregrineCrawler.lambda_handler")
            .timeout(Duration.minutes(1))
            .build();

        crawlerLambda.addEventSource(SnsEventSource.Builder.create(s3Topic).build());
        crawlerLambda.addToRolePolicy(PolicyStatement.Builder.create()
            .actions(Arrays.asList("glue:StartCrawler"))
            .effect(Effect.ALLOW)
            .resources(Arrays.asList("arn:aws:glue:"+this.getRegion()+":"+this.getAccount()+":crawler/"+peregrineCrawler.getRef()))
            .build());
        crawlerLambda.addToRolePolicy(listPeregrineBucket);
        crawlerLambda.addToRolePolicy(modifyPeregrineData);

        
        // Deploy sample data
        BucketDeployment peregrineDeploy = BucketDeployment.Builder.create(this, "peregrineData")
            .sources(Arrays.asList(Source.asset("data")))
            .destinationBucket(peregrineBucket)
            .exclude(Arrays.asList(".DS_Store"))
            .prune(false)
            .retainOnDelete(false)
            .build();
        
        // Ensure crawler exists prior to bucket deployment
        peregrineDeploy.getNode().addDependency(crawlerLambda);

        
        // Subscribe user email to the S3 upload topic
        s3Topic.addSubscription(EmailSubscription.Builder.create(emailAddress.getValueAsString()).build());  

        
        // Return S3 bucket name
        CfnOutput.Builder.create(this, "PeregrineBucket").exportName("peregrineBucket").description("Peregrine S3 Bucket").value(peregrineBucket.getBucketName()).build(); 
        

    }
}




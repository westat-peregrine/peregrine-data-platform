package com.westat;

import java.util.Arrays;
import java.util.Map;

import software.amazon.awscdk.core.CfnOutput;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.glue.*;
import software.amazon.awscdk.services.glue.CfnCrawler.S3TargetProperty;
import software.amazon.awscdk.services.glue.CfnCrawler.SchemaChangePolicyProperty;
import software.amazon.awscdk.services.glue.CfnCrawler.TargetsProperty;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.EventType;
import software.amazon.awscdk.services.s3.notifications.SnsDestination;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.EmailSubscription;
import software.amazon.awscdk.services.transfer.CfnServer;
import software.amazon.awscdk.services.transfer.CfnUser;
import software.amazon.awscdk.services.transfer.CfnUser.HomeDirectoryMapEntryProperty;

public class PeregrineStack extends Stack {
    public PeregrineStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public PeregrineStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // The code that defines your stack goes here

         // Add public SFTP endpoint and configure S3 storage
        /* CfnServer sftpEndpoint = CfnServer.Builder.create(this, "sftpEndpoint")
            .protocols(Arrays.asList("SFTP"))
            .endpointType("PUBLIC")
            .build(); */
        
        Bucket s3DemoBucket = Bucket.Builder.create(this, "s3DemoBucket")
            .bucketName("peregrine-"+this.getAccount())
            .encryption(BucketEncryption.S3_MANAGED)
            .removalPolicy(RemovalPolicy.DESTROY) 
            .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
            .build();

        // Create role for S3 access from AWS Transfer and Glue
        PolicyStatement listSftpBucket = new PolicyStatement();
        listSftpBucket.setSid("ListSFTPBucket");
        listSftpBucket.setEffect(Effect.ALLOW);
        listSftpBucket.addActions("s3:ListBucket");
        listSftpBucket.addResources(s3DemoBucket.getBucketArn());

        PolicyStatement modifySftpFolders = new PolicyStatement();
        modifySftpFolders.setSid("HomeDirObjectAccess");
        modifySftpFolders.setEffect(Effect.ALLOW);
        modifySftpFolders.addActions("s3:PutObject","s3:GetObject","s3:DeleteObject");
        modifySftpFolders.addResources(s3DemoBucket.getBucketArn()+"/*");

        PolicyDocument s3PolicyDocument = new PolicyDocument();
        s3PolicyDocument.addStatements(listSftpBucket, modifySftpFolders);

        /* Role s3TransferRole = Role.Builder.create(this, "s3TransferRole")
            .assumedBy(new ServicePrincipal("transfer.amazonaws.com"))
            .roleName("s3TransferRole")
            .description("Allows AWS Transfer to interact with S3 on behalf of users")
            .inlinePolicies(Map.of("s3TransferRolePolicy", s3PolicyDocument))
            .build(); */

        Role s3GlueRole = Role.Builder.create(this, "s3GlueRole")
            .assumedBy(new ServicePrincipal("glue.amazonaws.com"))
            .roleName("s3GlueRole")
            .description("Allows Glue to access objects stored in the SFTP bucket")
            .inlinePolicies(Map.of("s3GluePolicy",s3PolicyDocument))
            .build();

        // Create SFTP test user
        // TODO: Move to function that accepts parameters for username and ssh key and creates home folder

        /* HomeDirectoryMapEntryProperty mapping = HomeDirectoryMapEntryProperty.builder()
            .entry("/")
            .target("/"+sftpS3.getBucketName()+"/${transfer:UserName}")
            .build(); */

        /* CfnUser.Builder.create(this, "SFTPUser")
            .serverId(sftpEndpoint.getAttrServerId())
            .userName("test-user")
            .role(s3TransferRole.getRoleArn())
            .homeDirectoryType("LOGICAL")
            .homeDirectoryMappings(Arrays.asList(mapping))
            .sshPublicKeys(Arrays.asList("ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCeD6xLnwlwr5XtgmKiCG3ABy3sUzY4aE3EV7DUZKnRHWRWCiEX+HH84DTStr2UfxLGyoGXGMPcvU2NFTN8ttrxy7rzYlBFyldiU24XFQXcnnH86PRD6KcuHNbeAv3GM3XQtFvkUUfsY0fh5v8cnTxS3l6axT07WUxLF4/jM6g8O0nPefyYbWFmCjGs2RZZgs2JJ/eK7ukWghok2jpfd+vIOhiK5qebA+VWQxLcaLD6vti7k3kncB7l4kN+5KamUq47dzswpgS+BoRu9vfXyvMscPjvFfC9XGKMcNf0kEVgIZ9RCMeZW29WZ/TGJnb8zfbFPdz3GdwbRfx6sS59Vc43 wesnet\\ausborn_s@vmSAusborn"))
            .build(); */

        // Create Glue resources for access to SFTP data via Athena

        Database demoDatabase = Database.Builder.create(this,"demoDatabase")
            .databaseName("peregrine_demo")
            .build();

        CfnCrawler cfnDemoCrawler = CfnCrawler.Builder.create(this, "demoCrawler")
            .databaseName(demoDatabase.getDatabaseName())
            .targets(TargetsProperty.builder().s3Targets(Arrays.asList(S3TargetProperty.builder().path("s3://"+s3DemoBucket.getBucketName()).build())).build())
            .role(s3GlueRole.getRoleArn())
            .tablePrefix("peregrine_demo_")
            .configuration("{\"Version\":1,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"}}}")
            .schemaChangePolicy(SchemaChangePolicyProperty.builder().updateBehavior("LOG").build())
            .build();

        // Create s3 notification for uploads
        Topic s3Topic = Topic.Builder.create(this, "s3Topic")
            .displayName("Peregrine Upload Notification")    
            .build();

        s3DemoBucket.addEventNotification(EventType.OBJECT_CREATED_PUT, new SnsDestination(s3Topic));

        // Subscribe to the topic
        s3Topic.addSubscription(EmailSubscription.Builder.create("ScotAusborn@westat.com").build());  

        // Get output values for SFTP server and S3 bucket
        // CfnOutput.Builder.create(this, "sftpServer").exportName("sftpEndpoint").description("SFTP Server ID").value(sftpEndpoint.getAttrServerId()).build();
        CfnOutput.Builder.create(this, "Bucket").exportName("s3DemoBucket").description("Demo Bucket ID").value(s3DemoBucket.getBucketName()).build(); 
        
    }
}




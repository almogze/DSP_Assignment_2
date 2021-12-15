// import org.apache.hadoop.mapreduce.TestMapCollection.StepFactory;


/*
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
 */

// For the NEW SDK (V2):
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;


public class Main {
    public static S3Client S3;
    public static EmrClient emr;

    public static void main(String[] args){

/*
Meni's code (example)
        AWSCredentials credentials = new PropertiesCredentials();
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
                .withMainClass("some.pack.MainClass")
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://yourbucket/logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
 */




        // credentialsProvider = new EnvironmentVariableCredentialsProvider();

        // Bucket name.
        String bucketName = "razalmog2211";

        S3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        /*
        //delete the output file if it exist
        ObjectListing objects = S3.listObjects(bucketName, "outputAssignment2");
        for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
            S3.deleteObject(bucketName, s3ObjectSummary.getKey());
        }

        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        emr= AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        */

        System.out.println("Instantiating EMR instance!");
        EmrClient emrClient = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();

        // Print list of clusters (???).
        System.out.println( emr.listClusters());


        /*
        Notice that there is also the function "stepFactory(String bucket)" in com.amazonaws.services.elasticmapreduce.util.
        Maybe it is better for us. This one sets the bucket to: "us-east-1.elasticmapreduce".
        !@#!@#!@#
        */
        //StepFactory stepFactory = new StepFactory();
		/*
        step1
		 */

        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step1.jar")
                //.mainClass(myClass)
                .args("step1","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data")
                .build();

        StepConfig stepOne = StepConfig.builder()
                .hadoopJarStep(step1)
                .name("Step1")
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
/* Old version

        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/step1.jar")
                .withArgs("step1","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data");

        StepConfig stepOne = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
 */

		/*
        step2
		 */
        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step2.jar")
                .args("step2","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data")
                .build();

        StepConfig stepTwo = StepConfig.builder()
                .name("step2")
                .hadoopJarStep(step2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step3
		 */
        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step3.jar")
                .args("step3","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data")
                .build();

        StepConfig stepThree = StepConfig.builder()
                .name("step3")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step4
		 */
        HadoopJarStepConfig step4 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step4.jar")
                .args("step4")
                .build();

        StepConfig stepFour = StepConfig.builder()
                .name("step4")
                .hadoopJarStep(step4)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step5
		 */
        HadoopJarStepConfig step5 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step5.jar")
                .args("step5")
                .build();

        StepConfig stepFive = StepConfig.builder()
                .name("step5")
                .hadoopJarStep(step5)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step6
		 */
        HadoopJarStepConfig step6 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step6.jar")
                .args("step6","null","s3n://" + bucketName + "//outputAssignment2")
                .build();

        StepConfig stepSix = StepConfig.builder()
                .name("step6")
                .hadoopJarStep(step6)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(3)
                // Can also check what "InstanceType.M4_LARGE.toString()" returns and just put it here.
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("2.7.3")
                // PUT A NAME OF A KEYPAIR HERE!@#$!@#$!@#$
                .ec2KeyName("some key name. put one please!")
                // The bottom line may not be the one wanted, but it compiles.
                // Just beware that this may be the cause of problems -- Check it.
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();

        System.out.println("give the cluster all our steps");
        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("DSP_Ass_2")
                .instances(instances)
                .steps(stepOne,stepTwo,stepThree,stepFour,stepFive,stepSix)
                .logUri("s3n://" + bucketName + "/logs/")
/*
       Not sure if we need these 3 lines (not in Meni's example, although that doesn't mean anything).:

                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
*/
                .build();

        RunJobFlowResponse res = emr.runJobFlow(request);
        String id = res.jobFlowId();
        System.out.println("JobFlow Id: "+id);


    }

}

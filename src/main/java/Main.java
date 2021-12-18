// For the NEW SDK (V2):
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;


public class Main {
    public static S3Client S3;
    public static EmrClient emr;

    public static void main(String[] args){

        // Regarding "CredentialsProvider" (used by Meni) - I don't think we need it, but if we do
        // It's not that hard to add I think, just be aware that we may need it.

        // Bucket name.
        String bucketName = "razalmog2211";

        S3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

/*
        Add this if necessary.

        //delete the output file if it exist
        ObjectListing objects = S3.listObjects(bucketName, "outputDSPAss2");
        for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
            S3.deleteObject(bucketName, s3ObjectSummary.getKey());
        }
*/

        System.out.println("Instantiating EMR instance!");
        EmrClient emrClient = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();

        /*
        // Print list of clusters (see AWS specification for this).
        System.out.println( emr.listClusters());
         */

		/*
        step 1
        Add Short explanation of the functionality of step 1.
		 */


        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/Step1.jar")
                //.mainClass(myClass)
                .args("step1","null","s3://razalmog2211/test_text.txt")
                .build();
// s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data

        StepConfig stepOne = StepConfig.builder()
                .hadoopJarStep(step1)
                .name("Step1")
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

		/*
        step 2
        Add Short explanation of the functionality of step 2.
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
        step 3
        Add Short explanation of the functionality of step 3.
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
        step 4
        Add Short explanation of the functionality of step 4.
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
        step 5
        Add Short explanation of the functionality of step 5.
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
        step 6
        Add Short explanation of the functionality of step 6.
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
                .ec2KeyName("kp1")
                // The bottom line may not be the one wanted, but it compiles.
                // Just beware that this may be the cause of problems -- Check it.
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();


        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("DSP_Ass_2")
                .instances(instances)
                // .steps(stepOne,stepTwo,stepThree,stepFour,stepFive,stepSix)
                .steps(stepOne)
                .logUri("s3n://" + bucketName + "/logs/")
/*
       Not sure if we need these 3 lines (not in Meni's example, although that doesn't mean anything).:

                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
*/
                .build();

        System.out.println("Sending the job...");

        RunJobFlowResponse res = emr.runJobFlow(request);
        String jobFlowId = res.jobFlowId();
        System.out.println("Ran JobFlow with id: " + jobFlowId);


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

    }

}

import java.util.Arrays;

public class SandBox {

    static AwsBundle awsBundle = AwsBundle.getInstance();

    public static void main(String[] args){

        awsBundle.createBucketIfNotExists(AwsBundle.bucketName);

        awsBundle.putS3Object(AwsBundle.bucketName, "step1.jar", "step1.jar");
        awsBundle.putS3Object(AwsBundle.bucketName, "step2.jar", "step2.jar");
        awsBundle.putS3Object(AwsBundle.bucketName, "step3.jar", "step3.jar");
        awsBundle.putS3Object(AwsBundle.bucketName, "step4.jar", "step4.jar");
        awsBundle.putS3Object(AwsBundle.bucketName, "step5.jar", "step5.jar");
        awsBundle.putS3Object(AwsBundle.bucketName, "step6.jar", "step6.jar");

        /*
        String s1 = "I love bananas";
        String s2 = "Ilovebananas";
        String[] splits1 = s1.split(" ");
        String[] splits2 = s2.split(" ");
        System.out.println("len: " + splits1.length + Arrays.toString(splits1));
        System.out.println("len: " + splits2.length + Arrays.toString(splits2));
         */


    }

}
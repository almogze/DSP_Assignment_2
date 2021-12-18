


public class SandBox {

    static AwsBundle awsBundle = AwsBundle.getInstance();

    public static void main(String[] args){

        awsBundle.putS3Object(AwsBundle.bucketName, "Step1.jar", "Step1.jar");

    }

}
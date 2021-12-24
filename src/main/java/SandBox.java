


public class SandBox {

    static AwsBundle awsBundle = AwsBundle.getInstance();

    public static void main(String[] args){

<<<<<<< HEAD
        awsBundle.putS3Object(AwsBundle.bucketName, "Step1.jar", "Step1.jar");
=======
        String buckName = "razalmog2211";
        awsBundle.putS3Object(buckName, "step1.jar", "step1.jar");
>>>>>>> 9f5e23333d29b6cbfc85bed57721d2984108ff89

    }

}
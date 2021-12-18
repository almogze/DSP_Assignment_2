


public class SandBox {

    static AwsBundle awsBundle = AwsBundle.getInstance();

    public static void main(String[] args){

        String buckName = "razalmog2211";
        awsBundle.putS3Object(buckName, "Step1.jar", "Step1.jar");

    }

}
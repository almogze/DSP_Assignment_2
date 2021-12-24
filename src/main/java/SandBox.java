import java.util.Arrays;

public class SandBox {

    static AwsBundle awsBundle = AwsBundle.getInstance();

    public static void main(String[] args){

        String buckName = "razalmog2211";
        awsBundle.createBucketIfNotExists(buckName);
        // awsBundle.putS3Object(buckName, "step1.jar", "step1.jar");

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
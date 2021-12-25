import java.util.Arrays;

public class SandBox {

    static AwsBundle awsBundle = AwsBundle.getInstance();

    public static void main(String[] args){

        awsBundle.putS3Object(AwsBundle.bucketName, "step1.jar", "out/artifacts/DSP_Assignment_2_jar/DSP_Assignment_2.jar");

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
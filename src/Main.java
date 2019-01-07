import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.roberto.S3Client;

import java.io.*;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        Regions region = Regions.US_WEST_2;
        S3Client client = new S3Client(region);

        try {
            //read objects
            List<String> listOfBuckets = client.getListOfBuckets();
            System.out.println(listOfBuckets.toString());

            //upload a file
            String bucketName = "montynet-console-access";
            String fileNameInS3 = "consoleKey";
            String temporaryFile = "dedicated-tenancy-list";
            File file = createSampleFile(temporaryFile);
            String accountId = "123456789006";
            //client.uploadFile(bucketName, fileNameInS3, file);

            //read file contents
            InputStream objectContent = client.getObjectContent(bucketName, fileNameInS3);
            //displayTextInputStream(objectContent);

            //check for account in file


            boolean accountInFile = isAccountInFile(accountId, objectContent);

            // if the account is not in the file we write it "append" however it just makes a new
            // s3 object, there's now way to update an existing file in s3.
            System.out.println(accountInFile);
            if (!accountInFile){
                //once we know the account isn't in the file we have to read the stream again because
                // once the stream has been read it's no longer available
                InputStream inputStream = client.getObjectContent(bucketName, fileNameInS3);
                File newAppendedFile = createNewFileWithInputAsTextFile(inputStream, accountId, temporaryFile);
                client.uploadFile(bucketName, fileNameInS3, newAppendedFile);
            }


        }catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        catch (IOException ioe){
            System.out.println("Caught an IOException" + ioe.getMessage());
        }
    }

    private static File createSampleFile(String tempFileName) throws IOException {
        File file = File.createTempFile(tempFileName, ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("123456789001\n");
        writer.write("123456789002\n");
        writer.write("123456789003\n");
        writer.close();

        return file;
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null){
                break;
            }
            System.out.println("    " + line);
        }
        System.out.println();
    }

    private static boolean isAccountInFile(String accountId, InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        boolean found = false;
        while (true) {
            String line = reader.readLine();
            if (line == null){
                break;
            }
            if(line.equals(accountId)){
                found = true;
            }
        }
        return found;
    }

    /**
     * Creates a new file provided an inputstream of data. The accountId is appended to the end of the file.
     * @param input an InputStream that can be read.
     * @param accountId a string representing an account id.
     * @param fileName a temporary file name
     * @return a File object which has the original contents + appended string
     * @throws IOException
     */
    private static File createNewFileWithInputAsTextFile(InputStream input, String accountId, String fileName)
            throws IOException {
        File file = File.createTempFile(fileName, ".txt");
        file.deleteOnExit();
        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        String unixStyleNewLine = "\n";

        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while(true){
            String line = reader.readLine();
            if(line == null){
                break;
            }
            writer.write(line.endsWith(unixStyleNewLine) ? line : line + unixStyleNewLine);
        }

        writer.write(accountId + unixStyleNewLine);
        writer.close();

        return file;
    }
}


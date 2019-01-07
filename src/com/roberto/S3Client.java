package com.roberto;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class S3Client {

    private Region regionName;
    private AmazonS3 s3;

    /**
     * Sets up the client with specified region.
     * @param regionName valid region enum value.
     */
    public S3Client(Regions regionName){
        this.s3 = new AmazonS3Client();
        this.regionName = Region.getRegion(regionName);
        this.s3.setRegion(this.regionName);

    }

    /**
     * Creates a new bucket using the current client.
     * @param bucketName Unique name for bucket.
     * @throws AmazonServiceException thrown when request makes it to s3 but was rejected
     * @throws AmazonClientException thrown when client encounters serious internal problem when communicating with s3
     */
    public void createBucket(String bucketName) throws AmazonServiceException,
            AmazonClientException{
        this.s3.createBucket(bucketName);
    }

    /**
     * Fetches a list of bucket names.
     * @return A list of strings.
     * @throws AmazonServiceException
     * @throws AmazonClientException
     */
    public List<String> getListOfBuckets() throws AmazonServiceException, AmazonClientException{
        List<String> bucketNames = new ArrayList<String>();
        for(Bucket bucket : this.s3.listBuckets()){
            bucketNames.add(bucket.getName());
        }

        return bucketNames;
    }

    /**
     * Gets the inputStream for further processing by caller.
     * @param bucketName name of bucket
     * @param key unique identifier of file
     * @return
     * @throws AmazonServiceException
     * @throws AmazonClientException
     */
    public InputStream getObjectContent(String bucketName, String key) throws AmazonServiceException,
            AmazonClientException {
        S3Object obj = this.s3.getObject(new GetObjectRequest(bucketName, key));
        return obj.getObjectContent();
    }

    /**
     * Uploads a file to a bucket using the key as unique identifier.
     * @param bucketName bucket name
     * @param key unique identifier for file
     * @param fileToUpload File to upload
     * @throws AmazonServiceException
     * @throws AmazonClientException
     */
    public void uploadFile(String bucketName, String key, File fileToUpload) throws AmazonServiceException,
            AmazonClientException{
        this.s3.putObject(new PutObjectRequest(bucketName, key, fileToUpload));
    }

}

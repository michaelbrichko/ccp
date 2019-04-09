import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;

import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static String ClientRegion = "us-east-1";
    private static String BucketName = "bucket1-2fa54297";
    private static String WorkingDirectory = "C:/cloud_computing_project/";

    public static void main(String[] args) {

        // 2. Aviation on-time data
        String keyPrefix = "aviation/airline_ontime/";
        int yearStart = 1988;
        int yearEnd = 2008;
        String target2 = WorkingDirectory + "airline_ontime/";

        for (int i = yearStart; i <= yearEnd; i++) {
            for (int j = 1; j <= 12; j++ ) {

                ArrayList<String> cleanLinesAll = new ArrayList<>();
                if ((i == 1993 && j == 9) || (i == 1994 && j == 3) || (i == 2002 && j > 4) || (i == 2008 && j > 10)) {
                    continue; // datasets which doesn't present or corrupted
                }

                downloadFile(keyPrefix + i + "/On_Time_On_Time_Performance_" + i + "_" + j + ".zip", target2 + "ontime_" + i + "_" + j + ".zip");
                unzipFile(target2 + "ontime_" + i + "_" + j + ".zip", target2 + "ontime_" + i + "_" + j);
                cleanLinesAll.addAll(getCleanRowsOnTime(target2 + "ontime_" + i + "_" + j + "/On_Time_On_Time_Performance_" + i + "_" + j + ".csv", i, j));

                // write on time data
                writeData(cleanLinesAll, target2 + "ontime_clean.txt", true);
            }
        }

        zipFile(target2 + "ontime_clean.txt", target2 + "ontime_clean.txt.zip");
        uploadFile(target2 + "ontime_clean.txt.zip", "ontime_clean.txt.zip");
    }

   private static void downloadFile(String source, String target) {

       int maxRetries = 5;

       for (int r = 0; r < maxRetries; r++) {
           int timeout = 1000 * 60 * 30; // 30 min
           ClientConfiguration config = new ClientConfiguration();
           config.setConnectionTimeout(timeout);
           config.setSocketTimeout(timeout);
           config.setRequestTimeout(timeout);
           config.setClientExecutionTimeout(timeout);

           S3Object fullObject = null;
           try {
               AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                       .withRegion(ClientRegion)
                       .withCredentials(new ProfileCredentialsProvider())
                       .withClientConfiguration(config)
                       .build();

               System.out.println("Downloading an object. Source: " + source + " Target: " + target);
               fullObject = s3Client.getObject(new GetObjectRequest(BucketName, source));
               System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
               System.out.println("Content-Length: " + fullObject.getObjectMetadata().getContentLength());

               InputStream input = fullObject.getObjectContent();
               byte[] buffer = new byte[30 * 1024 * 1024]; // 30 mb

               try {
                   OutputStream output = new FileOutputStream(target);
                   try {
                       int bytesRead;
                       while ((bytesRead = input.read(buffer)) != -1) {
                           output.write(buffer, 0, bytesRead);
                       }
                   } finally {
                       output.close();
                   }
               } catch (FileNotFoundException e) {
                   e.printStackTrace();
               } catch (IOException e) {

                   if (r < maxRetries) {
                       continue;
                   }

                   e.printStackTrace();
               }

               break;

           } catch (AmazonServiceException e) {
               // The call was transmitted successfully, but Amazon S3 couldn't process
               // it, so it returned an error response.
               e.printStackTrace();
           } catch (SdkClientException e) {
               // Amazon S3 couldn't be contacted for a response, or the client
               // couldn't parse the response from Amazon S3.
               e.printStackTrace();
           } finally {
               // To ensure that the network connection doesn't remain open, close any open input streams.
               if (fullObject != null) {
                   try {
                       fullObject.close();
                   } catch (IOException e) {
                       e.printStackTrace();
                   }
               }
           }
       }
   }

   private static void uploadFile(String source, String target) {

       File file = new File(source);
       long contentLength = file.length();
       long partSize = 10 * 1024 * 1024; // Set part size to 10 MB.

       try {
           AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                   .withRegion(ClientRegion)
                   .withCredentials(new ProfileCredentialsProvider())
                   .build();

           // Create a list of ETag objects. You retrieve ETags for each object part uploaded,
           // then, after each individual part has been uploaded, pass the list of ETags to
           // the request to complete the upload.
           List<PartETag> partETags = new ArrayList<PartETag>();

           // Initiate the multipart upload.
           InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(BucketName, target);
           InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

           // Upload the file parts.
           long filePosition = 0;
           for (int i = 1; filePosition < contentLength; i++) {

               System.out.println("Uploading part: " + i);

               // Because the last part could be less than "partSize" MB, adjust the part size as needed.
               partSize = Math.min(partSize, (contentLength - filePosition));

               // Create the request to upload a part.
               UploadPartRequest uploadRequest = new UploadPartRequest()
                       .withBucketName(BucketName)
                       .withKey(target)
                       .withUploadId(initResponse.getUploadId())
                       .withPartNumber(i)
                       .withFileOffset(filePosition)
                       .withFile(file)
                       .withPartSize(partSize);

               // Upload the part and add the response's ETag to our list.
               UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
               partETags.add(uploadResult.getPartETag());

               filePosition += partSize;
           }

           // Complete the multipart upload.
           CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(BucketName, target, initResponse.getUploadId(), partETags);
           s3Client.completeMultipartUpload(compRequest);
       }
       catch(AmazonServiceException e) {
           // The call was transmitted successfully, but Amazon S3 couldn't process
           // it, so it returned an error response.
           e.printStackTrace();
       }
       catch(SdkClientException e) {
           // Amazon S3 couldn't be contacted for a response, or the client
           // couldn't parse the response from Amazon S3.
           e.printStackTrace();
       }
   }

    private static void unzipFile(String source, String target) {

        try {
            ZipFile zipFile = new ZipFile(source);

            zipFile.extractAll(target);
        } catch (ZipException e) {
            e.printStackTrace();
        }
    }
    private static void zipFile(String source, String target) {

        try {
            ZipFile zipFile = new ZipFile(target);
            ZipParameters parameters = new ZipParameters();

            parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
            parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);

            zipFile.createZipFile(new File(source), parameters);
        } catch (ZipException e) {
            e.printStackTrace();
        }
    }

    private static void writeData(ArrayList<String> data, String path, Boolean append) {

        FileWriter fileWriter = null;
        try {

            fileWriter = new FileWriter(path, append);

            for (String line : data) {
                fileWriter.append(line);
            }

            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ArrayList<String> getLineTokens(String line) {

        ArrayList<String> tokens = new ArrayList<>();

        char[] arr = line.toCharArray();

        StringBuilder token = new StringBuilder();
        Boolean processing = false;
        Boolean hasQuotes = false;
        for (char c : arr) {

            if (!processing) {

                processing = true;

                if (c == '\"') {
                    hasQuotes = true;
                    continue;  // in order to remove quotes from strings
                }
            } else {
                if (hasQuotes && c == '\"') {
                    hasQuotes = false;
                    continue;  // in order to remove quotes from strings
                }
            }

            if (c == ',' && !hasQuotes) {
                tokens.add(token.toString());
                hasQuotes = false;
                processing = false;
                token = new StringBuilder();
            } else if (c != ',' || hasQuotes){

                token.append(c);
            }
        }

        return tokens;
    }

    private static ArrayList<String> getCleanRowsOnTime(String source, int year, int month) {

        System.out.print("Cleaned file for year: " + year + ". month: " + month);

        ArrayList<String> cleanLines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(source))) {

            // find column number for UniqueCarrier, Origin, Dest, ArrDelay, DepDelay, CRSDepTime, FlightDate, Cancelled, FlightNum columns
            String[] headers = br.readLine().split(",");
            int arrDelayColumn = -1;
            int depDelayColumn = -1;
            int uniqueCarrierColumn = -1;
            int originColumn = -1;
            int destColumn = -1;
            int depTimeColumn = -1;
            int dateColumn = -1;
            int cancelledColumn = -1;
            int flightNumColumn = -1;

            for (int i = 0; i < headers.length; i++) {

                if (headers[i].equals("\"ArrDelay\"") && arrDelayColumn == -1) {
                    arrDelayColumn = i;
                }

                if (headers[i].equals("\"DepDelay\"") && depDelayColumn == -1) {
                    depDelayColumn = i;
                }

                if (headers[i].equals("\"UniqueCarrier\"") && uniqueCarrierColumn == -1) {
                    uniqueCarrierColumn = i;
                }

                if (headers[i].equals("\"Origin\"") && originColumn == -1) {
                    originColumn = i;
                }

                if (headers[i].equals("\"Dest\"") && destColumn == -1) {
                    destColumn = i;
                }

                if (headers[i].equals("\"CRSDepTime\"") && depTimeColumn == -1) {
                    depTimeColumn = i;
                }

                if (headers[i].equals("\"FlightDate\"") && dateColumn == -1) {
                    dateColumn = i;
                }

                if (headers[i].equals("\"Cancelled\"") && cancelledColumn == -1) {
                    cancelledColumn = i;
                }

                if (headers[i].equals("\"FlightNum\"") && flightNumColumn == -1) {
                    flightNumColumn = i;
                }
            }

            if (arrDelayColumn < 0 || depDelayColumn < 0 || uniqueCarrierColumn < 0 || originColumn < 0 || destColumn < 0 || depTimeColumn < 0 || dateColumn < 0 || cancelledColumn < 0 || flightNumColumn < 0) {
                throw new RuntimeException("ill-formed data. failed to find one of the expected columns");
            }

            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {

                ArrayList<String> rawLine = getLineTokens(line);

                if (headers.length != rawLine.size()) {
                    throw new RuntimeException("ill-formed data. Line: " + i + " Headers Length: " + headers.length + " Data length: " + rawLine.size());
                }

                int isCancelled = Float.parseFloat(rawLine.get(cancelledColumn)) == 1 ? 1 : 0;

                cleanLines.add(
                        rawLine.get(flightNumColumn) + "," +
                                rawLine.get(uniqueCarrierColumn) + "," +
                                rawLine.get(originColumn) + "," +
                                rawLine.get(destColumn) + "," +
                                rawLine.get(dateColumn) + "," +
                                rawLine.get(depTimeColumn) + "," +
                                rawLine.get(depDelayColumn) + "," +
                                rawLine.get(arrDelayColumn) + "," +
                                isCancelled +
                                "\n");

                i++;

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(". Lines count: " + cleanLines.size());

        return cleanLines;
    }
}

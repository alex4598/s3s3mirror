package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Vector;
import java.util.List;
import java.util.concurrent.*;
import org.apache.http.HttpStatus;

import static org.cobbzilla.s3s3mirror.MirrorConstants.*;

@Slf4j
public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }
    boolean forkPoolSemaphore = true;

    @Override
    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        long objectSize = summary.getSize();
        final MirrorOptions options = context.getOptions();
        final String sourceBucketName = options.getSourceBucket();
        final int maxPartRetries = options.getMaxRetries();
        final String targetBucketName = options.getDestinationBucket();
        final List<CopyPartResult> copyResponses = new Vector<CopyPartResult>();
        if (options.isVerbose()) {
            log.info("Initiating multipart upload request for " + summary.getKey());
        }
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(targetBucketName, keydest)
                .withObjectMetadata(sourceMetadata);

        if (options.isCrossAccountCopy()) {
            initiateRequest.withCannedACL(CannedAccessControlList.BucketOwnerFullControl);
        } else {
            initiateRequest.withAccessControlList(objectAcl);
        }

        final InitiateMultipartUploadResult initResult = client.initiateMultipartUpload(initiateRequest);

        final long optionsUploadPartSize = options.getUploadPartSize();
        long partSize = MirrorOptions.MINIMUM_PART_SIZE;
        
        if (optionsUploadPartSize == MirrorOptions.DEFAULT_PART_SIZE ) {
            final String eTag = summary.getETag();
            final int eTagParts = Integer.parseInt(eTag.substring(eTag.indexOf(MirrorOptions.ETAG_MULTIPART_DELIMITER.toString()) + 1,eTag.length()));

            long computedPartSize = MirrorConstants.MB;
            long minPartSize = objectSize;
            long maxPartSize = MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE;

            if (eTagParts > 1) {
                minPartSize = (long) Math.ceil((float)objectSize/eTagParts);
                maxPartSize = (long) Math.floor((float)objectSize/(eTagParts - 1));
            } 

            // Detect using standard MB and the power of 2
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = MirrorConstants.MB;
                while (computedPartSize < minPartSize) {
                    computedPartSize *= 2;
                }
            }

            // Detect other special cases like s3n and Aspera
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                for (int i = 0; i < MirrorOptions.SPECIAL_PART_SIZES.length; i++) {
                    computedPartSize = MirrorOptions.SPECIAL_PART_SIZES[i];
                    if (computedPartSize >= maxPartSize) {
                        break;
                    }
                }
            }

            // Detect if using 100MB increments
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 100 * MirrorConstants.MB;
                while (computedPartSize < minPartSize) {
                    computedPartSize += 100 * MirrorConstants.MB;
                }
            }

            // Detect if using 25MB increments up to 1GB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 25 * MirrorConstants.MB;
                while (computedPartSize < minPartSize && computedPartSize < 1 * MirrorConstants.GB) {
                    computedPartSize += 25 * MirrorConstants.MB;
                }
            }

            // Detect if using 10MB increments up to 1GB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 10 * MirrorConstants.MB;
                while (computedPartSize < minPartSize && computedPartSize < 1 * MirrorConstants.GB) {
                    computedPartSize += 10 * MirrorConstants.MB;
                }
            }

            // Detect if using 5MB increments up to 1GB
            if (computedPartSize < minPartSize || computedPartSize > maxPartSize) {
                computedPartSize = 5 * MirrorConstants.MB;
                while (computedPartSize < minPartSize && computedPartSize < 1 * MirrorConstants.GB) {
                    computedPartSize += 5 * MirrorConstants.MB;
                }
            }

            partSize = computedPartSize;

            if (computedPartSize > maxPartSize) {
                if (options.isVerbose()) {
                    log.info("Could not automatically determine part size for " + summary.getKey() + ", reverting to " + optionsUploadPartSize/MB + "MB" );
                }
                partSize = optionsUploadPartSize;
            }

            if (computedPartSize < MirrorOptions.MINIMUM_PART_SIZE) {
                if (options.isVerbose()) {
                    log.info("Part size of " + computedPartSize/MB + "MB for " + summary.getKey() + " is greater than AWS minimum of " + MirrorOptions.MINIMUM_PART_SIZE/MB + "MB");
                }
                partSize = MirrorOptions.MINIMUM_PART_SIZE;
            }

        } else {
            if (options.isVerbose()) {
                log.info("Using cli override part size of " + optionsUploadPartSize/MB + "MB for " + summary.getKey());
            }
            partSize = optionsUploadPartSize;
        }
        
        if (options.isVerbose()) {
            log.info("Part size for " + summary.getKey() + " is " + partSize/MB + "MB, with " + (int)Math.ceil((float)objectSize/partSize) + " parts.");
        }

        long bytePosition = 0;
        ForkJoinPool pool = new ForkJoinPool(MirrorOptions.MULTIPART_FORK_POOL_SIZE);
        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            
            final long lastByte_final = lastByte;
            final long bytePosition_final = bytePosition;
            final int i_final = i;
            
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    if (!forkPoolSemaphore)
                    {
                        return;
                    }
                    final String infoMessage = "copying bytes " + bytePosition_final + " to " + lastByte_final + " for " + summary.getKey();
                    if (options.isVerbose()) {
                        log.info(infoMessage);
                    }
                    CopyPartRequest copyRequest = new CopyPartRequest()
                            .withDestinationBucketName(targetBucketName)
                            .withDestinationKey(keydest)
                            .withSourceBucketName(sourceBucketName)
                            .withSourceKey(summary.getKey())
                            .withUploadId(initResult.getUploadId())
                            .withFirstByte(bytePosition_final)
                            .withLastByte(lastByte_final)
                            .withPartNumber(i_final);

                    for (int tries = 1; tries <= maxPartRetries; tries++) {
                        try {
                            if (options.isVerbose()) log.info("Try " + tries + ": " + infoMessage);
                            context.getStats().s3copyCount.incrementAndGet();
                            CopyPartResult copyPartResult = client.copyPart(copyRequest);
                            copyResponses.add(copyPartResult);
                            if (options.isVerbose()) log.info("Completed " + infoMessage);
                            break;
                        } catch (Exception e) {
                            if (tries == maxPartRetries) {
                                log.error("Retry threshold exceeded while " + infoMessage, e);
                                abortUpload(client, targetBucketName, initResult);
                            }
                        }
                    }
                }
            });
            bytePosition += partSize;
        }
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            abortUpload(client, targetBucketName, initResult);
        }
    
        if (!forkPoolSemaphore){
            return false;
        }
        
        if(options.isVerbose()) {
            log.info("Multipart request for " + summary.getKey() + " has " + copyResponses.size() + " parts");
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        client.completeMultipartUpload(completeRequest);
        if(options.isVerbose()) {
            log.info("Completed multipart request for " + summary.getKey());
        }
        context.getStats().bytesCopied.addAndGet(objectSize);
        return true;
    }
    
    private boolean abortUpload(AmazonS3Client client, String targetBucketName, InitiateMultipartUploadResult initResult){
        log.error("Something went wrong for " + summary.getKey() + ", aborting transfer!");
        client.abortMultipartUpload(new AbortMultipartUploadRequest(
                targetBucketName, keydest, initResult.getUploadId()));
        this.forkPoolSemaphore = false;
        return false;
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

}

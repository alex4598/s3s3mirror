package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MultipartKeyCopyJob extends KeyCopyJob {

    public MultipartKeyCopyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override
    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        long objectSize = summary.getSize();
        final MirrorOptions options = context.getOptions();
        String sourceBucketName = options.getSourceBucket();
        final int maxPartRetries = options.getMaxRetries();
        final String targetBucketName = options.getDestinationBucket();
        final List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
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

        long partSize = options.getUploadPartSize();
        long bytePosition = 0;
        List<Thread> threadPool = new ArrayList<Thread>();

        for (int i = 1; bytePosition < objectSize; i++) {
            long lastByte = bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1;
            final String infoMessage = "copying : " + bytePosition + " to " + lastByte;
            if (options.isVerbose()) {
                log.info(infoMessage);
            }
            final CopyPartRequest copyRequest = new CopyPartRequest()
                    .withDestinationBucketName(targetBucketName)
                    .withDestinationKey(keydest)
                    .withSourceBucketName(sourceBucketName)
                    .withSourceKey(summary.getKey())
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(i);

                    Thread copyThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            for (int tries = 1; tries <= maxPartRetries; tries++) {
                                try {
                                    if (options.isVerbose()) log.info("try :" + tries);
                                    context.getStats().s3copyCount.incrementAndGet();
                                    CopyPartResult copyPartResult = client.copyPart(copyRequest);
                                    copyResponses.add(copyPartResult);
                                    if (options.isVerbose()) log.info("completed " + infoMessage);
                                    break;
                                } catch (Exception e) {
                                    if (tries == maxPartRetries) {
                                        client.abortMultipartUpload(new AbortMultipartUploadRequest(
                                                targetBucketName, keydest, initResult.getUploadId()));
                                        log.error("Exception while doing multipart copy", e);
                                        return;
                                    }
                                }
                            }
                        }
                    });
                    copyThread.start();
                    threadPool.add(copyThread);
            bytePosition += partSize;
        }
        if (options.isVerbose()) {
            log.info(threadPool.size() + "Will spawn " + threadPool.size() + "threads for " + summary.getKey());
        }
        long start = System.currentTimeMillis();

        for (Thread t : threadPool) {
            try {
                t.join();
            } catch (InterruptedException e) {
                log.error("Something went terribly wrong!", e);
            }
        }
        long time = System.currentTimeMillis() - start;
        if (options.isVerbose()) {
            log.info(threadPool.size() + "threads finished after " + String.valueOf(time/1000) + " seconds.");
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucketName, keydest,
                initResult.getUploadId(), getETags(copyResponses));
        client.completeMultipartUpload(completeRequest);
        if(options.isVerbose()) {
            log.info("completed multipart request for : " + summary.getKey());
        }
        context.getStats().bytesCopied.addAndGet(objectSize);
        return true;
    }

    private List<PartETag> getETags(List<CopyPartResult> copyResponses) {
        List<PartETag> eTags = new ArrayList<PartETag>();
        for (CopyPartResult response : copyResponses) {
            eTags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return eTags;
    }

    @Override
    boolean objectChanged(ObjectMetadata metadata) {
        return summary.getSize() != metadata.getContentLength();
    }
}

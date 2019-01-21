package migrator

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3ObjectSummary
import java.io.ByteArrayInputStream
import java.util.*
import com.amazonaws.services.s3.model.PutObjectRequest


val metadataKey = "x-amz-website-redirect-location"

val AWS_KEY_TO = System.getenv("AWS_KEY_TO")
val AWS_KEY_FROM = System.getenv("AWS_KEY_FROM")
val AWS_SECRET_TO = System.getenv("AWS_SECRET_TO")
val AWS_SECRET_FROM = System.getenv("AWS_SECRET_FROM")
val BUCKET_FROM = System.getenv("BUCKET_FROM")
val BUCKET_TO = System.getenv("BUCKET_TO") ?: ""
val PREFIX_TO = System.getenv("PREFIX_TO") ?: ""

val REGION_TO = Regions.fromName(System.getenv("REGION_TO"))
val REGION_FROM = Regions.fromName(System.getenv("REGION_FROM"))

fun main(args: Array<String>) {

    val s3ClientFrom = AmazonS3ClientBuilder.standard()
        .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(AWS_KEY_FROM, AWS_SECRET_FROM)))
        .withRegion(REGION_FROM)
        .build()

    val s3ClientTo = AmazonS3ClientBuilder.standard()
        .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(AWS_KEY_TO, AWS_SECRET_TO)))
        .withRegion(REGION_TO)
        .build()

    val summaries = getDataFromS3(s3ClientFrom)
    val startTime = System.currentTimeMillis()
    summaries
        .map { it.key }
        .parallelStream()
        .map { s3ClientFrom.getObject(BUCKET_FROM, it) }
        .filter {
            it.objectMetadata.getRawMetadataValue(metadataKey) !== null
                    && it.objectMetadata.getRawMetadataValue(metadataKey).toString().indexOf("#") > 0
        }
        .map { mapOf("$PREFIX_TO${it.key}" to it.objectMetadata.getRawMetadataValue(metadataKey).toString().substringAfter("#")) }
        .forEach {
            it.forEach { key, value ->
                run {
                    println("Sending data for key = $key...")
                    val data = String(Base64.getDecoder().decode(value)).toByteArray()
                    val meta = ObjectMetadata().apply {
                        contentType = "application/json"
                        contentLength = data.size.toLong()
                    }
                    s3ClientTo.putObject(PutObjectRequest(BUCKET_TO, key, ByteArrayInputStream(data), meta))
                }
            }
        }

    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime

    println("*************************************")
    println("Done:")
    println("*************************************")
    println(duration.toString() + "ms")
    println("*************************************")
}


fun getDataFromS3(s3: AmazonS3): List<S3ObjectSummary> {
    var listing = s3.listObjects(BUCKET_FROM)
    val summaries = listing.objectSummaries
    while (listing.isTruncated) {
        listing = s3.listNextBatchOfObjects(listing)
        summaries.addAll(listing.objectSummaries)
    }
    return summaries
}

package migrator

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest


val BUCKET_FROM = System.getenv("BUCKET_FROM")
val BUCKET_TO = System.getenv("BUCKET_TO")
val SUFFIX_FROM = System.getenv("SUFFIX_FROM") ?: ""
val SUFFIX_TO = System.getenv("SUFFIX_TO") ?: ""

fun main() {
    val s3Client = S3Client.builder().region(Region.EU_WEST_1).build()
    val summaries = s3Client
        .listObjects(ListObjectsRequest.builder().bucket(BUCKET_FROM).build())
        .contents()
    val startTime = System.currentTimeMillis()
    summaries
        .map { it.key() }
        .subList(0, 10)
        .parallelStream()
        .forEach { key ->
            val getRequest = GetObjectRequest.builder()
                .bucket(BUCKET_FROM)
                .key(key)
                .build()
            val bytesResponse = s3Client.getObjectAsBytes(getRequest)
            println("Sending data for key = ${key}... . Time since start: ${System.currentTimeMillis() - startTime}ms")
            val putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_TO)
                .metadata(bytesResponse.response().metadata())
                .key(key)
                .build()
            s3Client.putObject(
                putRequest,
                RequestBody.fromInputStream(
                    bytesResponse.asByteArray().inputStream(),
                    bytesResponse.asByteArray().size.toLong()
                )
            )
        }

    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime

    println("*************************************")
    println("Done:")
    println("*************************************")
    println(duration.toString() + "ms")
    println("*************************************")
}
package migrator

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest


val BUCKET_FROM = System.getenv("BUCKET_FROM")
val BUCKET_TO = System.getenv("BUCKET_TO")

fun main() {
    val s3Client = S3Client.create()
    val summaries = s3Client
        .listObjects(ListObjectsRequest.builder().bucket(BUCKET_FROM).build())
        .contents()
    val startTime = System.currentTimeMillis()
    summaries
        .map { it.key() }
        .parallelStream()
        .forEach { key ->
            val getRequest = GetObjectRequest.builder()
                .bucket(BUCKET_FROM)
                .key(key)
                .build()
            val responseInputStream = s3Client.getObject(getRequest, ResponseTransformer.toInputStream())
            val s3Object = responseInputStream.response()
            println("Sending data for key = ${responseInputStream}...")
            val putRequest = PutObjectRequest.builder()
                .bucket(BUCKET_TO)
                .metadata(s3Object.metadata())
                .key(key)
                .build()
            s3Client.putObject(putRequest, RequestBody.fromInputStream(responseInputStream, s3Object.contentLength()))
        }

    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime

    println("*************************************")
    println("Done:")
    println("*************************************")
    println(duration.toString() + "ms")
    println("*************************************")
}
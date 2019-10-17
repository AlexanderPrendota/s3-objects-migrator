package migrator

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import java.net.URLEncoder
import java.nio.charset.StandardCharsets


val BUCKET_FROM = System.getenv("BUCKET_FROM")
val BUCKET_TO = System.getenv("BUCKET_TO")
val PREFIX_FROM = System.getenv("PREFIX_FROM") ?: ""
val PREFIX_TO = System.getenv("PREFIX_TO") ?: ""

fun main() {
    val startTime = System.currentTimeMillis()
    val s3Client = S3Client.builder().region(Region.EU_WEST_1).build()
    val summaries = getDataFromS3(s3Client)
    println("Got objects from $BUCKET_FROM. Count: ${summaries.size} Time since start: ${System.currentTimeMillis() - startTime}ms")
    summaries
        .map { it.key() }
        .parallelStream()
        .forEach { key ->
            val encodedUrl = URLEncoder.encode("$BUCKET_FROM/$key", StandardCharsets.UTF_8.toString())
            val newKey = prepareNewKey(key)
            println("Sending data for key = $key. New key = $newKey. Time since start: ${System.currentTimeMillis() - startTime}ms")
            val copyObjectRequest = CopyObjectRequest.builder()
                .copySource(encodedUrl)
                .bucket(BUCKET_TO)
                .key(newKey)
                .build()
            s3Client.copyObject(copyObjectRequest)
        }

    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime

    println("*************************************")
    println("Done:")
    println("*************************************")
    println(duration.toString() + "ms")
    println("*************************************")
}

fun getDataFromS3(s3Client: S3Client): List<S3Object> {
    val listObjectsReqManual = ListObjectsV2Request.builder()
        .bucket(BUCKET_FROM)
        .maxKeys(1000)
        .build()
    return s3Client.listObjectsV2Paginator(listObjectsReqManual).flatMap { it.contents() }
}


fun prepareNewKey(key: String): String {
    return if (PREFIX_FROM.isEmpty() || PREFIX_TO.isEmpty()) key
    else PREFIX_TO + key.removePrefix(PREFIX_FROM)
}
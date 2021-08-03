package org.smartcity.image.processor;

import java.io.InputStream;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@ApplicationScoped
public class ImageProcessor {

    private final static String TEMP_DIR = System.getProperty("java.io.tmpdir");

    private static Logger logger = LoggerFactory.getLogger(ImageProcessor.class);

    @Inject
    S3AsyncClient s3;

    @Inject
    @RestClient
    LicensePlateService service;

    @Inject
    @Channel("licenseplates")
    Emitter<String> licensePlateEmitter;

    @ConfigProperty(name = "bucket.name")
    String bucketName;

    @ConfigProperty(name = "toll.station")
    String station;

    @Incoming("images")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<CompletionStage<Void>> processImage(Message<String> message) {

        return Uni.createFrom().item(message)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().transform(m -> new JsonObject(message.getPayload()))
                .onItem().invoke(j -> logger.debug("Processing image " + j.getString("image")))
                .onItem().transformToUni(j -> Uni.createFrom().completionStage(s3.getObject(buildGetRequest(j.getString("image")), AsyncResponseTransformer.toBytes())))
                .onItem().transformToUni(r -> getLicensePlate(r.asInputStream()))
                .onItem().invoke(s -> logger.debug("Received response " + s))
                .onItem().transform(s -> processLicensePlate(s, new JsonObject(message.getPayload()).getLong("timestamp")))
                .onItem().transformToUni(this::lpEvent)
                .onItem().transform(v -> message.ack());
    }

    private Uni<Void> lpEvent(JsonObject event) {

        return Uni.createFrom().item(() -> {
            String key = event.getString("licenseplate");
            logger.debug("Sending message to license-plates channel. Key: " + key + " - Event = " + event);
            licensePlateEmitter.send(KafkaRecord.of(key, event.encode()));
            return null;
        });
    }


    private Uni<String> getLicensePlate(InputStream is) {
        return Uni.createFrom().item(() -> {
            MultipartBody body = new MultipartBody();
            body.file = is;
            return service.sendMultipartData(body);
        });
    }

    private JsonObject processLicensePlate(String lp, long timestamp) {
        String licenseplate = new JsonObject(lp).getString("lp");
        return new JsonObject().put("timestamp", timestamp).put("station", station).put("licenseplate", licenseplate)
                .put("status", licenseplate.length() >= 3 ? "success":"failed");
    }

    protected GetObjectRequest buildGetRequest(String objectKey) {
        return GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
    }
}

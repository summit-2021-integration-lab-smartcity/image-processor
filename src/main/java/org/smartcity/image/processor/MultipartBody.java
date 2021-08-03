package org.smartcity.image.processor;

import java.io.InputStream;
import javax.ws.rs.FormParam;
import javax.ws.rs.core.MediaType;

import org.jboss.resteasy.annotations.providers.multipart.PartFilename;
import org.jboss.resteasy.annotations.providers.multipart.PartType;

public class MultipartBody {

    @FormParam("image")
    @PartFilename("image")
    @PartType(MediaType.APPLICATION_OCTET_STREAM)
    public InputStream file;

}

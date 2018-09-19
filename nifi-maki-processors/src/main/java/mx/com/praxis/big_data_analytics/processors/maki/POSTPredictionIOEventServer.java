package mx.com.praxis.big_data_analytics.processors.maki;


import okhttp3.*;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.DateTime;
import org.json.JSONArray;

import org.json.JSONObject;

import java.io.*;
import java.util.*;

@Tags({"PredictionIO","Event Server","Event","Server","Maki"})
@CapabilityDescription("Ingesta en PredictionIO Event Server")
@SuppressWarnings("SpellCheckingInspection")

public class POSTPredictionIOEventServer extends AbstractProcessor {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private Connect connect;

    public static final PropertyDescriptor PREDICTIONIOAPPKEY= new PropertyDescriptor
            .Builder()
            .name("PredictionIO Event Server App Key")
            .displayName("PredictionIO Event Server App Key")
            .description("Event Server Key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREDICTIONIOHOST= new PropertyDescriptor
            .Builder()
            .name("PredictionIO Event Server Host")
            .displayName("PredictionIO Event Server Host")
            .description("Event Server Host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PREDICTIONIOAPPKEY);
        descriptors.add(PREDICTIONIOHOST);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        connect=Connect.getInstance();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        String app_key=context.getProperty(PREDICTIONIOAPPKEY).getValue().toString();
        String host=context.getProperty(PREDICTIONIOHOST).getValue().toString();

        FlowFile flowfile = session.get();
        if ( flowfile == null ) {
            return;
        }

        OkHttpClient client=connect.getClient();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {

                try{
                    String json = IOUtils.toString(in);

                    JSONObject jsonObj = new org.json.JSONObject(json);

                    String rfc=jsonObj.getJSONObject("cfdi:Comprobante").getJSONObject("cfdi:Receptor").get("rfc").toString();
                    String fecha=jsonObj.getJSONObject("cfdi:Comprobante").get("fecha").toString();
                    Object intervention=jsonObj.getJSONObject("cfdi:Comprobante").getJSONObject("cfdi:Conceptos").get("cfdi:Concepto");
                    JSONArray jArray;
                    JSONObject jObject;

                    if (intervention instanceof JSONArray) {
                        jArray = (JSONArray)intervention;

                        for (int i = 0; i < jArray.length(); ++i) {
                            JSONObject rec = jArray.getJSONObject(i);
                            Object unidad = rec.get("unidad");

                            String jsonF=formatJson(rfc,unidad.toString(),fecha).toString();
                            postEventServer(host,app_key,jsonF,client);
                        }
                    }
                    else if (intervention instanceof JSONObject) {
                        jObject = (JSONObject)intervention;
                        Object unidad = jObject.get("unidad");

                        String jsonF=formatJson(rfc,unidad.toString(),fecha).toString();
                        postEventServer(host,app_key,jsonF,client);
                    }

                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed ");
                    getLogger().debug("Error ");

                }
            }
        });

        session.transfer(flowfile, SUCCESS);
    }

    public static JSONObject formatJson(String rfc,String unidad,String fecha){
        JSONObject obj = new org.json.JSONObject();
        obj.put("event","buy");
        obj.put("entityType" , "user");
        obj.put("entityId" , rfc);
        obj.put("targetEntityType" , "item");
        obj.put("targetEntityId" , unidad);
        obj.put("eventTime" , DateTime.parse(fecha));
        return obj;
    }


    public static String postEventServer(String host, String app_key, String json,OkHttpClient client) throws IOException {
        String url=""+host+"/events.json?accessKey="+app_key+"";
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        Response response = client.newCall(request).execute();
        return response.body().string();
    }
}


class Connect{

    private static Connect instance;
    private static OkHttpClient client ;

    public synchronized static Connect getInstance() {
        if(instance==null){
            instance=new Connect();
        }
        return instance ;
    }

    private Connect() {
        client =  new OkHttpClient();
    }

    public synchronized  OkHttpClient getClient() {
        return client;
    }
}
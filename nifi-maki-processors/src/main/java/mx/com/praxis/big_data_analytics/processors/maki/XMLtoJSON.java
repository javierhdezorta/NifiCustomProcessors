package mx.com.praxis.big_data_analytics.processors.maki;


import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.json.JSONObject;
import org.json.XML;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@Tags({"XMLtoJSON", "XML", "JSON", "XML JSON", "XML TO JSON" })
@CapabilityDescription("Convierte un archivo XML a su reprensentación JSON")
@SuppressWarnings("SpellCheckingInspection")
public class XMLtoJSON extends AbstractProcessor {

    public XMLtoJSON() {
    }

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final String MATCH_ATTR = "match";

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                int PRETTY_PRINT_INDENT_FACTOR = 4;
                try{
                    String xml = IOUtils.toString(in);
                    getLogger().debug(xml);
                    JSONObject json = XML.toJSONObject(xml);

                    String jsonPrettyPrintString = json.toString(PRETTY_PRINT_INDENT_FACTOR);
                    getLogger().info(jsonPrettyPrintString);

                    if(json != null){
                        System.out.println("LD Json aplanado correctamente");
                    }

                    String result = json.toString();
                    getLogger().debug(result);
                    value.set(result);
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read xml string.");
                    getLogger().debug("Error en el parseo de XML a JSON");
                }
            }
        });

        String results = value.get();
        if(results != null && !results.isEmpty()){
            flowfile = session.putAttribute(flowfile, "match", results);
        }

        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        session.transfer(flowfile, SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
}

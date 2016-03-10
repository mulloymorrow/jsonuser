package test;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest

{
    @Test
    public void TestGenericRecord() throws IOException {
        String schemaString = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        String json = "{ \"name\" : \"joe\", \"favorite_number\":42, \"favorite_color\":\"blue\"}";

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);

        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord datum = reader.read(null, decoder);
        System.out.println("datum = " + datum);
        System.out.println("name = " + datum.get("name"));
        System.out.println("type = " + datum.getClass().getCanonicalName());
    }


    @Test
    public void TestSpecifcRecord() throws IOException {
        String schemaString = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

        Schema schema = new Schema.Parser().parse(schemaString);

        String json = "{ \"name\" : \"joe\", \"favorite_number\":42, \"favorite_color\":\"blue\"}";

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);

        DatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema);
        SpecificRecord datum = reader.read(null, decoder);
        System.out.println("datum = " + datum);
        //System.out.println("name = " + datum.get("name"));
        System.out.println("type = " + datum.getClass().getCanonicalName());
        User asUser = (User)datum;
        System.out.println("asUser = " + asUser.getName());
    }
}

package test;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


/**
 * Unit test for simple App.
 */
public class EncoderTests {

    private Schema schema;
    private ByteArrayOutputStream binaryStream;
    private BinaryEncoder binaryEncoder;
    private User datum;
    private JsonEncoder jsonEncoder;
    private ByteArrayOutputStream jsonStream;

    @Before
    public void setUp() throws Exception {
        String schemaString = "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
                "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
                " ]\n" +
                "}";
        schema = new Schema.Parser().parse(schemaString);
        binaryStream = new ByteArrayOutputStream();
        binaryEncoder = EncoderFactory.get().binaryEncoder(binaryStream, null);
        jsonStream = new ByteArrayOutputStream();
        jsonEncoder = EncoderFactory.get().jsonEncoder(schema, jsonStream);
        datum = User.newBuilder().setFavoriteColor("blue").setFavoriteNumber(2).setName("bob").build();

    }


    @Test
    public void TestGenericRecord() throws Exception {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(datum, binaryEncoder);
        binaryEncoder.flush();
        final GenericRecord genericRecordFromBinary = getGenericRecordFromBinary(binaryStream.toByteArray());
        System.out.println("genericRecordFromBinary = " + genericRecordFromBinary);

        writer.write(datum, jsonEncoder);
        jsonEncoder.flush();
        final GenericRecord genericRecordFromJson = getGenericRecordFromJson(jsonStream.toByteArray());
        System.out.println("genericRecordFromJson = " + genericRecordFromJson);

    }

    @Test
    public void TestSpecifcRecord() throws IOException {
        final DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(schema);
        writer.write(datum, binaryEncoder);
        binaryEncoder.flush();
        final SpecificRecord specificRecordFromBinary = getSpecificRecordFromBinary(binaryStream.toByteArray());
        System.out.println("specificRecordFromBinary = " + specificRecordFromBinary);

        writer.write(datum, jsonEncoder);
        jsonEncoder.flush();
        final SpecificRecord specificRecordFromJson = getSpecificRecordFromJson(jsonStream.toByteArray());
        System.out.println("specificRecordFromJson = " + specificRecordFromJson);
    }

    private GenericRecord getGenericRecordFromBinary(byte[] bytes) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }

    private GenericRecord getGenericRecordFromJson(byte[] bytes) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes));
        return reader.read(null, decoder);
    }

    private SpecificRecord getSpecificRecordFromBinary(byte[] bytes) throws IOException {
        DatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }

    private SpecificRecord getSpecificRecordFromJson(byte[] bytes) throws IOException {
        DatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes));
        return reader.read(null, decoder);
    }
}

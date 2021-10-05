package io.confluent.ps.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonHasherTest {
    ObjectMapper mapper = new ObjectMapper();
    @Test
    void testGenerateHash() throws JsonProcessingException {

        JsonNode jsonOne = mapper.readTree("{\"key1\":\"foo\",\"key2\":\"bar\",\"key3\":\"woof\"}");
        JsonNode jsonTwo = mapper.readTree("{\"key1\":\"foo\",\"key2\":\"bar\",\"key3\":\"woof\"}");
        JsonNode jsonThree = mapper.readTree("{\"key3\":\"woof\",\"key2\":\"bar\",\"key1\":\"foo\"}");
        JsonNode jsonFour = mapper.readTree("{\"key2\":\"bar\",\"key3\":\"woof\"}");

        String hashOne = JsonHasher.generateHash(jsonOne);
        String hashTwo = JsonHasher.generateHash(jsonTwo);
        String hashThree = JsonHasher.generateHash(jsonThree);
        String hashFour = JsonHasher.generateHash(jsonFour);

        assertEquals(hashOne, hashTwo);
        assertEquals(hashTwo, hashThree);
        assertNotEquals(hashOne, hashFour);
    }


    @Test
    void testRemoveNode() throws JsonProcessingException {
        JsonNode jsonOne = mapper.readTree(" {\"_id\": \"123\",\"_score\": 1, \"fields\": {\"removeMe\": [\"7.11.1\"]}} ");

        JsonHasher.removeNode(((ObjectNode)jsonOne), "/fields/removeMe");

        assertTrue(jsonOne.toString().equals("{\"_id\":\"123\",\"_score\":1,\"fields\":{}}"));

        JsonHasher.removeNode(((ObjectNode)jsonOne), "/_id");

        assertTrue(jsonOne.toString().equals("{\"_score\":1,\"fields\":{}}"));
    }
}
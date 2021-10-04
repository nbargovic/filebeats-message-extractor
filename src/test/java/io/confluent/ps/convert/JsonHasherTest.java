package io.confluent.ps.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonHasherTest {

    @Test
    void testGenerateHash() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
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
}
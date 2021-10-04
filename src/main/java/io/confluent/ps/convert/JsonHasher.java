package io.confluent.ps.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

/*
 * Generates a hash for a json object where ordering of elements in the object doesnt matter.
 * Example { e1:value1, e2:value2 } and { e2:value2, e1:value1 } will generate the same hash.
 */
public class JsonHasher {

    public static String generateHash(JsonNode jsonNode) {
        if (jsonNode.isObject()) {
            return objectHash((ObjectNode) jsonNode);
        } else if (jsonNode.isArray()) {
            return arrayHash((ArrayNode) jsonNode);
        } else {
            return sha256Hex(jsonNode.asText());
        }
    }

    private static String objectHash(ObjectNode objectNode) {
        final Iterable<String> fieldIter = () -> objectNode.fieldNames();
        final String concatenated = StreamSupport.stream(fieldIter.spliterator(), false)
                .sorted() //make sure to sort the field names
                .map(fieldName -> sha256Hex(fieldName) + generateHash(objectNode.get(fieldName)))
                .collect(Collectors.joining());
        return sha256Hex(concatenated);
    }

    private static String arrayHash(ArrayNode arrayNode) {
        final Iterable<JsonNode> nodeIter = () -> arrayNode.iterator();
        final String concatenatedHash = StreamSupport.stream(nodeIter.spliterator(), false)
                .map(node -> generateHash(node))
                .collect(Collectors.joining());
        return sha256Hex(concatenatedHash);
    }

}

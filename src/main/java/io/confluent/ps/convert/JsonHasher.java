package io.confluent.ps.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

/*
 * Generates a hash for a json object where ordering of elements in the object doesnt matter.
 * Example { e1:value1, e2:value2 } and { e2:value2, e1:value1 } will generate the same hash.
 */
public class JsonHasher {
    private final static Logger log = LoggerFactory.getLogger(JsonHasher.class);

    public static String generateHash(JsonNode jsonNode) {
        log.debug("Generating hash id for " + jsonNode.toString() );
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

    /*
     * Here because removing json elements using a simple string path is easier this way.
     */
    public static void removeNode(ObjectNode fullJson, String path) {
        log.debug("Removing " + path + " json metadata event.");
        if (Objects.isNull(path) || path.isEmpty()) {
            return;
        } else {
            String[] pathElements = path.split("/");
            String fieldToRemove = pathElements[pathElements.length - 1];
            if (pathElements.length > 1){
                String parentPath = path.substring(0,path.lastIndexOf("/"));
                ((ObjectNode)fullJson.at(parentPath)).remove(fieldToRemove);
            }
            else{
                fullJson.remove(fieldToRemove);
            }
        }
    }

}

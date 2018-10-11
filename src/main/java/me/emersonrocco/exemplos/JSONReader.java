package me.emersonrocco.exemplos;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONReader {

    private static final Logger logger = LoggerFactory.getLogger(JSONReader.class);


    private static Object nodeValueToObject(JsonNode node) { //No child objects or arrays in this flat data just text/number
        switch (node.getNodeType()) {
            case NUMBER:
                if (node.isFloat() || node.isDouble()) {
                    return new Double(node.doubleValue());
                } else {
                    //For simplicity let all integers be Long.
                    return new Long(node.asLong());
                }
            case STRING:
                return node.asText();
            case BOOLEAN:
                return node.asBoolean();
            case NULL:
                return null;
            default:
                logger.warn("Tipo de No desconhecido:" + node.getNodeType());
                return null;
        }
    }


    public static DataObject readObject(String objectName, String json) {
        DataObject datum = new DataObject(objectName);
        JsonFactory factory = new JsonFactory();
        try {
            JsonParser parser = factory.createParser(json);
            parser.setCodec(new ObjectMapper());
            while (!parser.isClosed()) {
                JsonToken token = parser.nextToken();

                if (token != null && token.equals(JsonToken.START_OBJECT)) {

                    JsonNode jsonTree = parser.readValueAsTree();
                    jsonTree.fields().forEachRemaining(entry -> {
                        if (entry.getValue() != null) {
                            Object value = nodeValueToObject(entry.getValue());
                            if (value == null) {
                                //  NOOP - unknown node type
                            } else {
                                datum.addColumnValue(entry.getKey(), nodeValueToObject(entry.getValue()));
                            }
                        } else {
                            logger.warn("Valor Nulo para entrada : " + entry.getKey());
                        }
                    });
                }
            }
        } catch (Exception e) {
            logger.error("Erro no parser", e);
        }
        return datum;
    }


}
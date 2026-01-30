package com.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageParserTest {
    @Test
    void parse_extracts_core_fields() {
        String json = "{\n" +
                "  \"priTenant\": \"BIOM\",\n" +
                "  \"taskId\": \"t-001\",\n" +
                "  \"adviseKey\": \"ORDER_CREATE\",\n" +
                "  \"produceTime\": 1700000000000,\n" +
                "  \"processedTime\": 1700000004000,\n" +
                "  \"internalSeconds\": 4,\n" +
                "  \"transRequest\": {\n" +
                "    \"systemNo\": \"SYS-A\",\n" +
                "    \"operDetail\": {\"nodeName\": \"node-1\"},\n" +
                "    \"businessProcess\": {\"busId\": \"B-01\", \"busVer\": \"v1\"}\n" +
                "  },\n" +
                "  \"result\": \"SUCCESS\"\n" +
                "}";

        MessageParser parser = new MessageParser(new ObjectMapper());
        ParsedMessage msg = parser.parse(json);

        Assertions.assertEquals("t-001", msg.getTaskId());
        Assertions.assertEquals("BIOM", msg.getTenant());
        Assertions.assertEquals("SYS-A", msg.getSystemNo());
        Assertions.assertEquals("ORDER_CREATE", msg.getAdviseKey());
        Assertions.assertEquals("node-1", msg.getNodeName());
        Assertions.assertEquals("B-01", msg.getBusId());
        Assertions.assertEquals("v1", msg.getBusVer());
        Assertions.assertEquals("SUCCESS", msg.getResult());
        Assertions.assertEquals(1700000000000L, msg.getProduceTimeMs());
        Assertions.assertEquals(1700000004000L, msg.getProcessedTimeMs());
        Assertions.assertEquals(4.0, msg.getInternalSeconds());
    }
}

package com.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageParserTest {
    @Test
    void parse_extracts_core_fields() {
        // 测试 STATE 类型消息
        String stateJson = "{" +
                "\"taskId\": \"SUNYARDBP26011515382155320260129180226778644436\"," +
                "\"tenant\": \"SUNYARD\"," +
                "\"transNo\": \"KAFKA2030\"," +
                "\"watchState\": \"5\"," +
                "\"serverIp\": \"240e:473:600:1adf:b04b:59e4:e134:71d7\"," +
                "\"nodeName\": \"AT4\"," +
                "\"workitemId\": \"5230054\"," +
                "\"processId\": \"BC260115151732098\"" +
                "}";

        MessageParser parser = new MessageParser(new ObjectMapper());
        // Key uses state prefix
        ParsedMessage msg = parser.parse("state/test-task/uuid", stateJson);

        Assertions.assertEquals("STATE", msg.getMessageType());
        Assertions.assertEquals("state", msg.getCategory());
        Assertions.assertEquals("SUNYARDBP26011515382155320260129180226778644436", msg.getTaskId());
        Assertions.assertEquals("SUNYARD", msg.getTenant());
        Assertions.assertEquals("5", msg.getWatchState());
        Assertions.assertEquals("AT4", msg.getNodeName());
        Assertions.assertEquals("SUCCESS", msg.getResult()); // watchState=5 映射为 SUCCESS
    }

    @Test
    void parse_competence_message() {
        String competenceJson = "{" +
                "\"errorCode\": \"1503\"," +
                "\"errorType\": \"5\"," +
                "\"errorTaskId\": \"SUNYARDBP26011515382155320260127180033346625608\"," +
                "\"errorApp\": \"SUNYARD\"," +
                "\"errorLevel\": \"4\"," +
                "\"errorInfo\": \"产品系统实现类加锁异常\"," +
                "\"errorInterface\": \"ETCD4000\"," +
                "\"serverIp\": \"240e:473:600:1adf:84e9:7007:4a81:178e\"" +
                "}";

        MessageParser parser = new MessageParser(new ObjectMapper());
        ParsedMessage msg = parser.parse("competence/test-task/uuid", competenceJson);

        Assertions.assertEquals("COMPETENCE", msg.getMessageType());
        Assertions.assertEquals("competence", msg.getCategory());
        Assertions.assertEquals("SUNYARDBP26011515382155320260127180033346625608", msg.getTaskId());
        Assertions.assertEquals("SUNYARD", msg.getTenant());
        Assertions.assertEquals("1503", msg.getErrorCode());
        Assertions.assertEquals("5", msg.getErrorType());
        Assertions.assertEquals("4", msg.getErrorLevel());
        Assertions.assertEquals("产品系统实现类加锁异常", msg.getErrorInfo());
    }

    @Test
    void parse_tenant_message() {
        String tenantJson = "{" +
                "\"priTenant\": \"SUNYARD\"," +
                "\"taskId\": \"SUNYARDBP26011515382155320260129175254051282744\"," +
                "\"adviseKey\": \"BP260115153821553-2-BC260115173243197-AT5\"," +
                "\"transRequest\": {" +
                "  \"systemNo\": \"AGENT\"," +
                "  \"userNo\": \"AT5\"," +
                "  \"operDetail\": {" +
                "    \"workitemId\": \"5230032\"," +
                "    \"startTime\": \"20260129175256\"," +
                "    \"nodeName\": \"AT5\"," +
                "    \"workitemState\": \"1\"" +
                "  }" +
                "}," +
                "\"systemInfo\": {" +
                "  \"processId\": \"BC260115173243197\"," +
                "  \"workitemId\": 5230032," +
                "  \"state\": \"WaitForCheckOut\"" +
                "}" +
                "}";

        MessageParser parser = new MessageParser(new ObjectMapper());
        ParsedMessage msg = parser.parse("AGENT/test-task/KAFKA4000/0/123/uuid", tenantJson);

        Assertions.assertEquals("TENANT", msg.getMessageType());
        Assertions.assertEquals("AGENT", msg.getCategory());
        Assertions.assertEquals("SUNYARDBP26011515382155320260129175254051282744", msg.getTaskId());
        Assertions.assertEquals("SUNYARD", msg.getTenant());
        Assertions.assertEquals("AGENT", msg.getSystemNo());
        Assertions.assertEquals("AT5", msg.getNodeName());
        Assertions.assertEquals("WaitForCheckOut", msg.getSystemState());
        Assertions.assertEquals("1", msg.getWorkitemState());
    }

    @Test
    void parse_tenant_message_with_wrong_key_prefix() {
        String tenantJson = "{" +
                "\"priTenant\": \"SUNYARD\"," +
                "\"taskId\": \"SUNYARDBP26011515382155320260129175254051282744\"," +
                "\"transRequest\": {" +
                "  \"systemNo\": \"AGENT\"," +
                "  \"userNo\": \"AT5\"," +
                "  \"operDetail\": {" +
                "    \"nodeName\": \"AT5\"" +
                "  }" +
                "}" +
                "}";

        MessageParser parser = new MessageParser(new ObjectMapper());
        // Key uses SUNYARD prefix but systemNo is AGENT
        ParsedMessage msg = parser.parse("SUNYARD/test-task/KAFKA4000/0/123/uuid", tenantJson);

        Assertions.assertEquals("TENANT", msg.getMessageType());
        // Should be corrected to AGENT
        Assertions.assertEquals("AGENT", msg.getCategory());
        Assertions.assertEquals("AGENT", msg.getSystemNo());
    }

    @Test
    void parse_tenant_message_uses_systemNo_as_category() {
        String tenantJson = "{" +
                "\"priTenant\": \"SUNYARD\"," +
                "\"taskId\": \"SUNYARDBP26011515382155320260129175254051282744\"," +
                "\"transRequest\": {" +
                "  \"systemNo\": \"CUSTOM_SYS\"," +
                "  \"userNo\": \"AT5\"," +
                "  \"operDetail\": {" +
                "    \"nodeName\": \"AT5\"" +
                "  }" +
                "}" +
                "}";

        MessageParser parser = new MessageParser(new ObjectMapper());
        // Key uses SUNYARD prefix
        ParsedMessage msg = parser.parse("SUNYARD/test-task/KAFKA4000/0/123/uuid", tenantJson);

        Assertions.assertEquals("TENANT", msg.getMessageType());
        // Should be corrected to CUSTOM_SYS
        Assertions.assertEquals("CUSTOM_SYS", msg.getCategory());
        Assertions.assertEquals("CUSTOM_SYS", msg.getSystemNo());
    }
}

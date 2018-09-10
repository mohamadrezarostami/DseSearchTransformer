/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.iais.dsesearchtransformer;

import com.datastax.bdp.search.solr.FieldOutputTransformer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * @author mohamadreza
 */
public class JsonFieldOutputTransformer extends FieldOutputTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonFieldOutputTransformer.class);

    @Override
    public void stringField(FieldInfo fieldInfo, String value, StoredFieldVisitor visitor, DocumentHelper helper) throws IOException {

        LOGGER.info("name: " + fieldInfo.name + ", value: " + value);
        try {
            if (fieldInfo.name.equals("value")) {
                FieldInfo response = helper.getFieldInfo("responsiblities");
                JsonElement parse = new JsonParser().parse(value);
                if (parse.getAsJsonObject().get("responsiblities").getAsJsonArray().toString() != null) {
                    visitor.stringField(response, parse.getAsJsonObject().get("responsiblities").getAsJsonArray().toString().getBytes());
                }
                visitor.stringField(helper.getFieldInfo("value"), value.getBytes());
            }
        } catch (IOException e) {
            LOGGER.error(fieldInfo.name + " " + e.getMessage());
            throw e;
        }
    }
}

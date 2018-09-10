/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.iais.dsesearchtransformer;

import com.datastax.bdp.search.solr.FieldInputTransformer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.lucene.document.Document;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 *
 * @author mohamadreza
 */
public class JsonFieldInputTransformer extends FieldInputTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonFieldInputTransformer.class);

    @Override
    public boolean evaluate(String field) {
        return field.equals("value");
    }

    @Override
    public void addFieldToDocument(SolrCore core, IndexSchema schema, String key, Document doc, SchemaField fieldInfo, String fieldValue, DocumentHelper helper) throws IOException {

        try {
            LOGGER.info("JsonFieldInputTransformer called");
            LOGGER.info("fieldValue: " + fieldValue);

            SchemaField responsiblities = core.getLatestSchema().getFieldOrNull("responsiblities");
            JsonElement parse = new JsonParser().parse(fieldValue);
            helper.addFieldToDocument(core, core.getLatestSchema(), key, doc, responsiblities, parse.getAsJsonObject().get("responsiblities").getAsJsonArray().toString());
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
            throw new RuntimeException(ex);
        }
    }
}

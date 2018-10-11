package me.emersonrocco.exemplos;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class DataTransform {

    private static int BIGQUERY_NESTING_LIMIT = 1000;
    private static final Logger logger = LoggerFactory.getLogger(DataTransform.class);


    public static PCollection<String> loadText(Pipeline p, String name) {
        Options options = (Options) p.getOptions();
        String loadingBucket = options.getLoadingBucketURL();
        String objectToLoad = storedObjectName(loadingBucket, name);
        return p.apply(TextIO.Read.named(name).from(objectToLoad));
    }


    private static String storedObjectName(String loadingBucket, String name) {
        StringBuilder builder = new StringBuilder();
        builder.append(loadingBucket).append("/").append(name).append(".json");
        return builder.toString();
    }



    public static PCollection<DataObject> innerJoin(String name, PCollection<KV<Long,
            DataObject>> table1, PCollection<KV<Long, DataObject>> table2) {
        final TupleTag<DataObject> t1 = new TupleTag<DataObject>();
        final TupleTag<DataObject> t2 = new TupleTag<DataObject>();
        PCollection<KV<Long, CoGbkResult>> joinedResult = group(name, table1, table2, t1, t2);
        PCollection<List<DataObject>> mergedResult = joinedResult.apply("merge join results", MapElements.via((KV<Long, CoGbkResult> group) -> {
            List<DataObject> result = new ArrayList<DataObject>();
            Iterable<DataObject> leftObjects = group.getValue().getAll(t1);
            Iterable<DataObject> rightObjects = group.getValue().getAll(t2);
            leftObjects.forEach((DataObject l) -> {
                logger.debug("l:" + l.toString());
                rightObjects.forEach((DataObject r) -> {

                    result.add(l.duplicate().merge(r));
                });
            });
            return result;
        }).withOutputType(new TypeDescriptor<List<DataObject>>() {
        }));
// [END mergeJoinResults]
// [START flattenMergedResults]
        return mergedResult.apply(new Flatten.FlattenIterables<>());
// [END flattenMergedResults]
    }


    private static PCollection<KV<Long, CoGbkResult>> group(String name,
                                                            PCollection<KV<Long, DataObject>> first,
                                                            PCollection<KV<Long, DataObject>> second,
                                                            TupleTag<DataObject> firstTag,
                                                            TupleTag<DataObject> secondTag
    ) {
        final CoGroupByKey<Long> grouper = CoGroupByKey.create();
        PCollection<KV<Long, CoGbkResult>> joinedResult;
        try {
            joinedResult = KeyedPCollectionTuple.of(firstTag, first)
                    .and(secondTag, second)
                    .apply(name, grouper);
        } catch (Exception e) {
            logger.error("exception grouping.", e);
            return null;
        }
        return joinedResult;
    }


    public static PCollection<KV<Long, DataObject>> loadTable(Pipeline p, String name, String keyName,
                                                                 LookupDescription... mappers) {
        PCollection<String> text = loadText(p, name);
        return loadTableFromText(text, name, keyName, mappers);
    }

    public static LookupDescription lookup(String objectName, String keyKey, String valueKey, String... destinationKeys) {
        return new LookupDescription(objectName, keyKey, valueKey, destinationKeys);
    }



    public static PCollection<KV<Long, DataObject>> loadTableFromText(PCollection<String> text, String name, String keyName) {
        final String namespacedKeyname = name + "_" + keyName;
        return text.apply("load " + name, MapElements.via((String input) -> {
            DataObject datum = JSONReader.readObject(name, input);
            Long key = (Long) datum.getColumnValue(namespacedKeyname);
            return KV.of(key, datum);
        }).withOutputType(new TypeDescriptor<KV<Long, DataObject>>() {
        }));
    }
    // [END loadTableByValue]

    public static PCollection<KV<Long, DataObject>> loadTableFromText(PCollection<String> text, String name,
                                                                                 String keyName,
                                                                                 LookupDescription... mappers) {
        //[START lookupTableWithSideInputs2]
        Map<String, PCollectionView<Map<Long, String>>> mapSideInputs = new HashMap<String, PCollectionView<Map<Long, String>>>();
        for (LookupDescription mapper : mappers) {
            PCollectionView<Map<Long, String>> mapView = loadMap(text.getPipeline(), mapper.objectName, mapper.keyKey, mapper.valueKey);
            mapper.destinationKeys.forEach((destinationKey) -> {
                mapSideInputs.put(name + "_" + destinationKey, mapView);
            });
        }
        //[END lookupTableWithSideInputs2]
        return loadTableFromText(text, name, keyName, mapSideInputs);
    }

    static PCollection<KV<Long, DataObject>> loadTableFromText(PCollection<String> text, String name,
                                                                          String keyName,
                                                                          Map<String, PCollectionView<Map<Long, String>>> mappings) {
        final String namespacedKeyname = name + "_" + keyName;

        return text.apply(ParDo.named("load with mappings").of(new DoFn<String, KV<Long, DataObject>>() {
            @Override
            public void processElement(ProcessContext processContext) throws Exception {
                String input = processContext.element();
                DataObject result = JSONReader.readObject(name, input);
                mappings.forEach((String key, PCollectionView<Map<Long, String>> mapping) -> {
                    //[START lookupTableWithSideInputs3]
                    Map<Long, String> sideInputMap = processContext.sideInput(mapping);
                    Long id = (Long) result.getColumnValue(key);
                    if (id != null) {
                        String label = (String) sideInputMap.get(id);
                        if (label == null) {
                            label = "" + id;
                        }
                        result.replace(key, label);
                        //[END lookupTableWithSideInputs3]
                    }
                });

                Long key = (Long) result.getColumnValue(namespacedKeyname);
                processContext.output(KV.of(key, result));
            }
        }).withSideInputs(mappings.values()));
    }
    /**
     * Given a PCollection of String's each representing an MusicBrainzDataObject transform those strings into
     * MusicBrainzDataObject's where the namespace for the MusicBrainzDataObject is 'name'
     *
     * @param text the json string representing the MusicBrainzDataObject
     * @param name the namespace for hte MusicBrainzDataObject
     * @return PCollection of MusicBrainzDataObjects
     */
    public static PCollection<DataObject> loadTableFromText(PCollection<String> text, String name) {
        return text.apply("load : " + name, MapElements.via((String input) -> JSONReader.readObject(name, input))
                .withOutputType(new TypeDescriptor<DataObject>() {
                }));
    }

    public static PCollectionView<Map<Long, String>> loadMapFromText(PCollection<String> text, String keyKey, String valueKey) {
        String keyKeyName = "_" + keyKey;
        String valueKeyName = "_" + valueKey;

        PCollection<KV<Long, String>> entries = text.apply(MapElements.via((String input) -> {
            DataObject object = JSONReader.readObject("", input);
            Long key = (Long) object.getColumnValue(keyKeyName);
            String value = (String) object.getColumnValue(valueKeyName);
            return KV.of(key, value);
        }).withOutputType(new TypeDescriptor<KV<Long, String>>() {
        }));

        return entries.apply(View.<Long, String>asMap());
    }

    private static PCollectionView<Map<Long, String>> loadMap(Pipeline p, String name, String keyKey, String valueKey) {
        PCollection<String> text = loadText(p, name);
        return loadMapFromText(text, keyKey, valueKey);

    }

    public static class LookupDescription {
        String objectName;
        List<String> destinationKeys;
        String keyKey;
        String valueKey;


        LookupDescription(String objectName, String keyKey, String valueKey, String... destinationKeys) {
            this.objectName = objectName;
            this.destinationKeys = Arrays.asList(destinationKeys);
            this.keyKey = keyKey;
            this.valueKey = valueKey;
        }

    }

    private static Map<String, Object> serializeableTableSchema(TableSchema schema) {
        return serializeableTableSchema(null, schema.getFields());
    }
    private static Map<String, Object> serializeableTableSchema(Map<String, Object> current, List<TableFieldSchema> fields) {
        if (current == null) {
            current = new HashMap<String, Object>();
        }
        for (TableFieldSchema field : fields) {
            if (field.getType().equals(FieldSchemaListBuilder.RECORD)) {
                current.put(field.getName(), serializeableTableSchema(null, field.getFields()));
            } else {
                current.put(field.getName(), field.getType());
            }
        }
        return current;
    }



    public static PCollection<TableRow> transformToTableRows(PCollection<DataObject> objects, TableSchema schema) {
        Map<String, Object> serializableSchema = serializeableTableSchema(schema);
        return objects.apply("Big Query TableRow Transform",
                MapElements.via((DataObject inputObject) -> {
                    List<TableRow> rows = toTableRows(inputObject, serializableSchema);
                    return rows;
                }).withOutputType(new TypeDescriptor<List<TableRow>>() {
                })).apply(new Flatten.FlattenIterables<>());
    }

    private static List<TableRow> toTableRows(DataObject mbdo, Map<String, Object> serializableSchema) {
        TableRow row = new TableRow();
        List<TableRow> result = new ArrayList<TableRow>();
        Map<String, List<DataObject>> nestedLists = new HashMap<String, List<DataObject>>();
        Set<String> keySet = serializableSchema.keySet();
        /*
         *  construct a row object without the nested objects
         */
        int maxListSize = 0;
        for (String key : keySet) {
            Object value = serializableSchema.get(key);
            Object fieldValue = mbdo.getColumnValue(key);
            if (fieldValue != null) {
                if (value instanceof Map) {
                    List<DataObject> list = (List<DataObject>) fieldValue;
                    if (list.size() > maxListSize) {
                        maxListSize = list.size();
                    }
                    nestedLists.put(key, list);
                } else {
                    row.set(key, fieldValue);
                }

            }
        }
        /*
         * add the nested objects but break up the nested objects across duplicate rows if nesting limit exceeded
         */
        TableRow parent = row.clone();
        Set<String> listFields = nestedLists.keySet();
        for (int i = 0; i < maxListSize; i++) {
            parent = (parent == null ? row.clone() : parent);
            final TableRow parentRow = parent;
            nestedLists.forEach((String key, List<DataObject> nestedList) -> {
                if (nestedList.size() > 0) {
                    if (parentRow.get(key) == null) {
                        parentRow.set(key, new ArrayList<TableRow>());
                    }
                    List<TableRow> childRows = (List<TableRow>) parentRow.get(key);
                    childRows.add(toChildRow(nestedList.remove(0), (Map<String, Object>) serializableSchema.get(key)));
                }
            });
            if ((i > 0) && (i % BIGQUERY_NESTING_LIMIT == 0)) {
                result.add(parent);
                parent = null;
            }
        }
        if (parent != null) {
            result.add(parent);
        }
        return result;
    }

    private static TableRow toChildRow(DataObject object, Map<String, Object> childSchema) {
        TableRow row = new TableRow();
        childSchema.forEach((String key, Object value) -> {
            row.set(key, object.getColumnValue(key));
        });
        return row;
    }

    public static PCollection<KV<Long, DataObject>> by(String name, PCollection<DataObject> input) {
        return input.apply("by " + name, MapElements.via(
                (DataObject inputObject) -> {
                    try {
                        return KV.of((Long) inputObject.getColumnValue(name), inputObject);
                    } catch (Exception e) {
                        logger.error(" exception in by " + name, e);
                        return null;
                    }
                }).withOutputType(new TypeDescriptor<KV<Long, DataObject>>() {
        }));
    }
}
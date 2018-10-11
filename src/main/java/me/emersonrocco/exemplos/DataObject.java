package me.emersonrocco.exemplos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DataObject implements Serializable {

    private String namespace;
    private Map<String, Object> columns = new HashMap<String, Object>();


    public DataObject(String namespace) {
        this.namespace = namespace;
    }

    public String getNamespace() {
        return namespace;
    }

    public Iterator<Map.Entry<String, Object>> getColumns() {
        return columns.entrySet().iterator();
    }

    public void addColumnValue(String name, Object value) {
        String namespaced_name = getNamespace() + "_" + name;
        columns.put(namespaced_name, value);
    }

    public Object removeColumnValue(String name) {
        return columns.remove(name);
    }

    public Object getColumnValue(String name) {
        return columns.get(name);
    }

    public DataObject merge(DataObject other) {

        if (other != null) {
            other.columns.forEach((String key, Object value) -> {
                if (columns.containsKey(key)) {
                    System.out.println("Chave duplicada - Fazendo Merge de dados ");
                }
                columns.put(key, value);
            });
        }
        return this;
    }

    public void replace(String key, Object value) {
        columns.replace(key, value);
    }


    public DataObject duplicate() {
        DataObject duplicate = new DataObject(namespace);
        duplicate.columns = (HashMap<String, Object>) ((HashMap<String, Object>) columns).clone();
        return duplicate;
    }

}
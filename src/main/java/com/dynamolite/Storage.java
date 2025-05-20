package com.dynamolite;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage handles data persistence and retrieval with version vectors for conflict detection.
 */
public class Storage {
    private static final Logger logger = LoggerFactory.getLogger(Storage.class);
    private final Map<String, Value> data;
    private final String dataDir;
    private static final String DATA_FILE = "storage.dat";

    public Storage(String dataDir) {
        this.dataDir = dataDir;
        this.data = new ConcurrentHashMap<>();
        createDataDirectory();
        loadData();
    }

    private void createDataDirectory() {
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /**
     * Stores a value with its version vector
     */
    public void put(String key, String value, VersionVector version) {
        data.put(key, new Value(value, version));
        saveData();
    }

    /**
     * Retrieves a value and its version vector
     */
    public Value get(String key) {
        return data.get(key);
    }

    /**
     * Removes a key-value pair
     */
    public void remove(String key) {
        data.remove(key);
        saveData();
    }

    /**
     * Loads data from disk
     */
    @SuppressWarnings("unchecked")
    private void loadData() {
        File file = new File(dataDir, DATA_FILE);
        if (!file.exists()) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            Map<String, Value> loadedData = (Map<String, Value>) ois.readObject();
            data.putAll(loadedData);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error loading data: {}", e.getMessage());
        }
    }

    /**
     * Saves data to disk
     */
    private void saveData() {
        File file = new File(dataDir, DATA_FILE);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
            oos.writeObject(data);
        } catch (IOException e) {
            logger.error("Error saving data: {}", e.getMessage());
        }
    }

    /**
     * Value class to store both the data and its version vector
     */
    public static class Value implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String data;
        private final VersionVector version;

        public Value(String data, VersionVector version) {
            this.data = data;
            this.version = version;
        }

        public String getData() {
            return data;
        }

        public VersionVector getVersion() {
            return version;
        }
    }
} 
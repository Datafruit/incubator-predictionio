package org.apache.predictionio.data.storage.druid.directory.hdfs;

import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DocListCollector extends SimpleCollector {
    List<Integer> list = new ArrayList<>();

    @Override
    public void collect(int doc) throws IOException {
        list.add(doc);
    }

    public List<Integer> getDocList() {
        return list;
    }

    @Override
    public boolean needsScores() {
        return false;
    }
}

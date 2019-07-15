package lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;


/** Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing.
 * Run it with no command-line arguments for usage information.
 */
public class indexer {

    private indexer() {}

    public static void main(String[] args) {
        JSONObject obj = new JSONObject();
        obj.put("key","id");
        obj.put("key_value","1");
        obj.put("store",Store.NO);
        JSONObject obj1 = new JSONObject();
        obj1.put("key","name");
        obj1.put("key_value","nandakumar");
        obj1.put("store",Store.NO);
        JSONObject obj2 = new JSONObject();
        obj2.put("name","nandakumar");
        obj2.put("id","1");
        JSONArray arr = new JSONArray();
        arr.add(obj);
        arr.add(obj1);

        createIndexData("DataIndex", obj2.toJSONString(), arr);
//        indexValue();
        //indexFile("IndexDirectoryFiles",false);
    }
    private static void createIndexData(String path,String content, JSONArray indexValues){
        try {
            Date start = new Date();
            Directory indexDir;
            if (System.getProperty("os.name").toLowerCase().contains("windows")){
                indexDir = FSDirectory.open(Paths.get(path));
            }else{
                indexDir = NIOFSDirectory.open(Paths.get(path));
            }
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setOpenMode(OpenMode.CREATE);
            IndexWriter writer = new IndexWriter(indexDir, iwc);
            Document doc = new Document();
            for (Object indexValue : indexValues){
                JSONObject data = (JSONObject) indexValue;
                doc.add(new StringField((String) (data.get("key")), (String) data.get("key_value"),(Store) data.get("store")));
//                writer.updateDocument(new Term((String) data.get("key"), content), doc);
            }
            doc.add(new StringField("data", content, Store.YES));
            writer.addDocument(doc);
            writer.close();
            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void indexValue(){
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("db.txt"));
            String line = reader.readLine();
            Date start = new Date();
            Directory indexDir;
            if (System.getProperty("os.name").toLowerCase().contains("windows")){
                indexDir = FSDirectory.open(Paths.get("index"));
            }else{
                indexDir = NIOFSDirectory.open(Paths.get("index"));
            }
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            IndexWriter writer = new IndexWriter(indexDir, iwc);
            boolean flag = true;
            while (line != null){
                if (!line.equals("")){
                    JSONObject data = (JSONObject) (new JSONParser().parse(line));
                    Document doc = new Document();
                    doc.add(new StringField("username", (String) data.get("username"), Store.NO));
                    doc.add(new StringField("email", (String) data.get("email"), Store.NO));
                    doc.add(new StringField("status", (String) data.get("status"), Store.NO));
                    doc.add(new StringField("details", line, Store.YES));
                    if (flag || writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                        flag = false;
                        writer.addDocument(doc);
                    }
                    else{
                        writer.updateDocument(new Term("details", line), doc);
                    }

                }
                line = reader.readLine();
            }
            writer.close();
            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /* Index all text files under a directory. */
    private static void indexFile(String docsPath, boolean create) {
        String indexPath = "index";
        if (docsPath == null) {
            System.err.println("Usage: doc not found");
            System.exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            if (create) {
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer.  But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            IndexWriter writer = new IndexWriter(dir, iwc);
            indexDocs(writer, docDir);

            // NOTE: if you want to maximize search performance,
            // you can optionally call forceMerge here.  This can be
            // a terribly costly operation, so generally it's only
            // worth it when your index is relatively static (ie
            // you're done adding documents to it):
            //
            // writer.forceMerge(1);

            writer.close();

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() +
                    "\n with message: " + e.getMessage());
        }
    }

    /**
     * Indexes the given file using the given writer, or if a directory is given,
     * recurses over files and directories found under the given directory.
     *
     * NOTE: This method indexes one document per input file.  This is slow.  For good
     * throughput, put multiple documents into your input file(s).  An example of this is
     * in the benchmark module, which can create "line doc" files, one document per line,
     * using the
     * <a href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be stored
     * @param path The file to index, or the directory to recurse into to find files to index
     * @throws IOException If there is a low-level I/O error
     */
    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            Document doc = new Document();

            // Add the path of the file as a field named "path".  Use a
            // field that is indexed (i.e. searchable), but don't tokenize
            // the field into separate words and don't index term frequency
            // or positional information:
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.toFile())));
            String line = reader.readLine();

            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            Field datetime = new StringField("date", "2222", Field.Store.YES);
            doc.add(pathField);
            doc.add(datetime);

            // Add the last modified date of the file a field named "modified".
            // Use a LongPoint that is indexed (i.e. efficiently filterable with
            // PointRangeQuery).  This indexes to milli-second resolution, which
            // is often too fine.  You could instead create a number based on
            // year/month/day/hour/minutes/seconds, down the resolution you require.
            // For example the long value 2011021714 would mean
            // February 17, 2011, 2-3 PM.
            doc.add(new LongPoint("modified", lastModified));

            // Add the contents of the file to a field named "contents".  Specify a Reader,
            // so that the text of the file is tokenized and indexed, but not stored.
            // Note that FileReader expects the file to be in UTF-8 encoding.
            // If that's not the case searching for special characters will fail.
            doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                // New index, so we just add the document (no old document can be there):
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                // Existing index (an old copy of this document may have been indexed) so
                // we use updateDocument instead to replace the old one matching the exact
                // path, if present:
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        }
    }
}
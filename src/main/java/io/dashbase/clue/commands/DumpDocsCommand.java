package io.dashbase.clue.commands;

import io.dashbase.clue.LuceneContext;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;

@Readonly
public class DumpDocsCommand extends ClueCommand {

    private final LuceneContext ctx;

    public DumpDocsCommand(LuceneContext ctx) {
        super(ctx);
        this.ctx = ctx;
    }

    @Override
    public String getName() {
        return "dumpdocs";
    }

    @Override
    public String help() {
        return "dumps all the stored fields for all document";
    }

    @Override
    protected ArgumentParser buildParser(ArgumentParser parser) {
        parser.addArgument("-f", "--fields").required(false).help("only dumps fields with given names (comma separated)");
        return parser;
    }

    @Override
    public void execute(Namespace args, PrintStream out) throws Exception {
        String fields = args.getString("fields");
        HashSet<String> fieldsSet = new HashSet<String>();
        if (fields != null) {
            fieldsSet.addAll(Arrays.asList(fields.split(",", 0)));
        }
        IndexReader reader = ctx.getIndexReader();
        List<LeafReaderContext> leaves = reader.leaves();
        for (int i = 0; i <= reader.numDocs() ; ++i) {
            out.println("doc:" + i);
            for (LeafReaderContext ctx : leaves) {
                LeafReader atomicReader = ctx.reader();

                int docID = i - ctx.docBase;

                if (docID >= atomicReader.maxDoc()) {
                    continue;
                }

                if (docID >= 0) {
                    Document storedData = atomicReader.document(docID);

                    if (storedData == null) continue;

                    for (IndexableField indexableField : storedData.getFields()) {
                        if (fieldsSet.size() > 0 && !fieldsSet.contains(indexableField.name())) {
                            continue;
                        }
                        out.print(indexableField.name() + ":");
                        final Number number = indexableField.numericValue();
                        if (number != null) {
                            out.println(number);
                            continue;
                        }

                        final String strData = indexableField.stringValue();

                        if (strData != null) {
                            out.println(strData);
                            continue;
                        }

                        final BytesRef bytesRef = indexableField.binaryValue();
                        if (bytesRef != null) {
                            out.println(bytesRef);
                        }

                        out.println("<unsupported value type>");
                    }
                }
            }
        }
    }
}

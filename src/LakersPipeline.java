import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

public class LakersPipeline {

    private static final Pipeline PIPELINE = Pipeline.create();
    private static final String POSITIONS_ROSTER_PATH =
            "src/assets/data/lakers_roster_positions_2023.csv";
    private static final String COLLEGES_ROSTER_PATH =
            "src/assets/data/lakers_roster_colleges_2023.csv";
    private static final String OUTPUT_PATH =
            "src/assets/data/lakers_combined_roster_2023.csv";

    /* Runs the pipeline */
    public static void runPipeline(){
        buildPipeline();
        PIPELINE.run().waitUntilFinish();
    }

    /* Helper to assemble the pipeline */
    private static void buildPipeline() {
        // First, extract the raw CSV data into PCollections
        PCollection<String> rosterPositions = null;
        PCollection<String> rosterColleges = null;
        try {
            rosterPositions = extractCSV(POSITIONS_ROSTER_PATH);
            rosterColleges = extractCSV(COLLEGES_ROSTER_PATH);
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }

        // Convert to collections of key-value pairs based on player name
        PCollection<KV<String, String>> rosterPositionsKV = rosterPositions.apply(
                "Convert to KV",
                MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(new MapToStringKV()));
        PCollection<KV<String, String>> rosterCollegesKV = rosterColleges.apply(
                "Convert to KV",
                MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(new MapToStringKV()));

        // Join the PCollections and aggregate all the values by player name
        PCollection<KV<String, CoGbkResult>> rostersCombined =
                KeyedPCollectionTuple.of(new TupleTag<>(), rosterPositionsKV)
                        .and(new TupleTag<>(), rosterCollegesKV)
                        .apply(CoGroupByKey.create());

        // Print results
        printStringKVCollection(rostersCombined);
    }

    /* Helper static class to define a mapping of String to KV<String, String> for roster data */
    private static class MapToStringKV implements SerializableFunction<String, KV<String, String>> {
        @Override
        public KV<String, String> apply(String input) {
            String key = input.split(",")[0];
            String value = input.split(",")[1];
            return KV.of(key, value);
        }
    }

    /* Helper to read a given CSV file to a PCollection */
    private static PCollection<String> extractCSV(String path) throws FileNotFoundException {
        return PIPELINE.apply("Read Roster CSV", TextIO.read().from(path));
    }

    /* Helper to print a PCollection with KV String Paids */
    private static void printStringKVCollection(PCollection<KV<String, CoGbkResult>> collection) {
        // Convert the elements to uppercase.
        collection.apply("Print",
                ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));
    }

    public static void main(String[] args) {
        LakersPipeline.runPipeline();
    }
}
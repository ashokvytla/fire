package fire.fileformats.json;

import org.apache.spark.api.java.JavaSparkContext;

import fire.sparkutil.CreateSparkContext;

public class JsonUtil {
    public static void main(String[] args) throws Exception {

        // process arguments
        if (args.length < 0) {
            System.err.println("Usage: JsonUtil ");
            System.exit(1);
        }

        JavaSparkContext ctx = CreateSparkContext.create(args);

        System.out.println("=== Data source: JSON Dataset ===");
        // A JSON dataset is pointed by path.
        // The path can be either a single text file or a directory storing text files.
        String path = "examples/src/main/resources/people.json";
        // Create a DataFrame from the file(s) pointed by path
        // DataFrame peopleFromJsonFile = sqlCtx.jsonFile(path);

        ctx.stop();
    }
}

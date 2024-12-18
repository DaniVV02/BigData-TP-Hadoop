
import java.io.IOException;
import java.time.Instant;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


// =========================================================================
// COMPARATEURS
// =========================================================================

/**
 * Comparateur qui inverse la méthode de comparaison d'un sous-type T de WritableComparable (ie. une clé).
 */
@SuppressWarnings("rawtypes")
class InverseComparatorProfit<T extends WritableComparable> extends WritableComparator {

    public InverseComparatorProfit(Class<T> parameterClass) {
        super(parameterClass, true);
    }

    /**
     * Cette fonction définit l'ordre de comparaison entre 2 objets de type T.
     * Dans notre cas nous voulons simplement inverser la valeur de retour de la méthode T.compareTo.
     *
     * @return 0 si a = b <br>
     *         x > 0 si a > b <br>
     *         x < 0 sinon
     */
    @SuppressWarnings("unchecked")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        // On inverse le retour de la méthode de comparaison du type
        return -a.compareTo(b);
    }
}

/**
 * Inverseur de la comparaison du type Text.
 */
class TextInverseComparatorProfit extends InverseComparator<Text> {

    public TextInverseComparatorProfit() {
        super(Text.class);
    }
}


/**
 * Comparateur pour trier les profits en ordre décroissant.
 */
class ProfitDescendingComparator extends WritableComparator {

    public ProfitDescendingComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable da = (DoubleWritable) a;
        DoubleWritable db = (DoubleWritable) b;

        return -da.compareTo(db); // Tri décroissant des profits
    }
}



// =========================================================================
// CLASSE MAIN
// =========================================================================

public class TriAvecComparaisonParProfit {
    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/9-TriAvecComparaisonParPROFITCroissant-";
    private static final Logger LOG = Logger.getLogger(TriAvecComparaisonParProfit.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }


    // =========================================================================
    // MAPPER
    // =========================================================================

    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length > 20) {
                String customerId = fields[5]; // Customer ID
                String customerName = fields[6]; // Customer Name
                String profit = fields[20]; // Profit

                try {
                    double profitValue = Double.parseDouble(profit);
                    context.write(new DoubleWritable(profitValue), new Text(customerId + "," + customerName));                } catch (NumberFormatException e) {
                    // Ignorer les lignes avec des profits non valides
                }
            }
        }

    }

    // =========================================================================
    // REDUCER
    // =========================================================================
/*
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalProfit = 0;

            for (DoubleWritable value : values) {
                totalProfit += value.get();
            }

            context.write(key, new DoubleWritable(totalProfit));
        }

    }

 */

    public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // Afficher l'identifiant du client et son profit total
                context.write(value, key);
            }
        }

    }

    // =========================================================================
    // MAIN
    // =========================================================================

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "9-Sort");

        /*
         * Affectation de la classe du comparateur au job.
         * Celui-ci sera appelé durant la phase de shuffle.
         */

        // Pour tri décroissant, le mettre en commentaire si tri croissant
        //job.setSortComparatorClass(TextInverseComparator.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        // Enlever le commentaire pour decroissant
        //job.setSortComparatorClass(ProfitDescendingComparator.class); // pour appliquer le tri sur le profit


        job.waitForCompletion(true);
    }
}
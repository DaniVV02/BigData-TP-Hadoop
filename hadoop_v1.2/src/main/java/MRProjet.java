
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRProjet {
    private static final String INPUT_PATH = "input-Projet/";
    private static final String OUTPUT_PATH = "output/requete-";
    private static final Logger LOG = Logger.getLogger(MRProjet.class.getName());

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

    // MAP

    public static class PurchasesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Adaptez selon le séparateur
            String dateAchats = fields[0];   // Date_achats
            String joueur = fields[1];      // IdJoueurs
            String categorie = fields[2];   // TypeAchat
            String montantAchat = fields[3]; // Montant_achat
            context.write(new Text(dateAchats + "," + joueur + "," + categorie), new Text("1," + montantAchat));
        }
    }


    public static class SessionStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Adaptez selon votre format
            String joueur = fields[1]; // IdJoueurs
            String nombreAchats = fields[2]; // Nombre_achats
            String dureeSession = fields[3]; // Duree_Session_Minutes
            String nombreParties = fields[4]; // Nombre_parties
            context.write(new Text(joueur), new Text(nombreAchats + "," + dureeSession + "," + nombreParties));
        }
    }

    public static class EventPurchasesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Adaptez selon votre format
            String idEvenement = fields[0]; // IdEvenement
            String dateAchat = fields[1]; // Date_achats
            String nombreAchats = fields[2]; // Nombre_achats
            String periode = fields[3]; // pre, pendant ou post

            context.write(new Text(idEvenement), new Text(periode + "," + nombreAchats));
        }
    }

    public static class CharacterSessionsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String personnageSaison = fields[1] + "," + fields[2]; // Nom du brawler, Saison
            context.write(new Text(personnageSaison), new IntWritable(1));
        }
    }

    public static class VictoryRateMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String personnageSaison = fields[1] + "," + fields[2]; // Nom du brawler, Saison
            String tauxVictoire = fields[3]; // Taux de victoire
            context.write(new Text(personnageSaison), new Text(tauxVictoire + ",1"));
        }
    }


    // REDUCEE

    public static class PurchasesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAchats = 0;
            double montantTotal = 0.0;

            for (Text value : values) {
                String[] counts = value.toString().split(",");
                totalAchats += Integer.parseInt(counts[0]);
                montantTotal += Double.parseDouble(counts[1]);
            }
            context.write(key, new Text(totalAchats + "," + montantTotal));
        }
    }


    public static class SessionStatsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAchats = 0;
            double totalDureeSession = 0.0;
            double totalParties = 0.0;
            int count = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                totalAchats += Integer.parseInt(fields[0]);
                totalDureeSession += Double.parseDouble(fields[1]);
                totalParties += Double.parseDouble(fields[2]);
                count++;
            }

            double moyenneDureeSession = totalDureeSession / count;
            double moyenneParties = totalParties / count;

            context.write(key, new Text(totalAchats + "," + moyenneDureeSession + "," + moyenneParties));
        }
    }

    public static class EventPurchasesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int achatsPre = 0, achatsPendant = 0, achatsPost = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                String periode = fields[0];
                int nombreAchats = Integer.parseInt(fields[1]);

                if ("pre".equals(periode)) {
                    achatsPre += nombreAchats;
                } else if ("pendant".equals(periode)) {
                    achatsPendant += nombreAchats;
                } else if ("post".equals(periode)) {
                    achatsPost += nombreAchats;
                }
            }

            context.write(key, new Text(achatsPre + "," + achatsPendant + "," + achatsPost));
        }
    }

    public static class CharacterSessionsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalSessions = 0;

            for (IntWritable value : values) {
                totalSessions += value.get();
            }

            context.write(key, new IntWritable(totalSessions));
        }
    }

    public static class VictoryRateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalTauxVictoire = 0.0;
            int count = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                totalTauxVictoire += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }

            double moyenneTauxVictoire = totalTauxVictoire / count;

            context.write(key, new Text(String.valueOf(moyenneTauxVictoire)));
        }
    }







    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job pour 1ere requete du 1er datamart
        Job job1 = new Job(conf, "Req1");
        job1.setJarByClass(MRProjet.class);
        job1.setMapperClass(PurchasesMapper.class);
        job1.setReducerClass(PurchasesReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH + "datamart1.csv"));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH + "req1-" + Instant.now().getEpochSecond()));
        job1.waitForCompletion(true);

        // Job pour 2e requete du 1er datamart
        Job job2 = new Job(conf, "Req2");
        job2.setJarByClass(MRProjet.class);
        job2.setMapperClass(SessionStatsMapper.class);
        job2.setReducerClass(SessionStatsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(INPUT_PATH + "datamart1.csv"));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH + "req2-" + Instant.now().getEpochSecond()));
        job2.waitForCompletion(true);

        // Job pour 3e requete du 1er datamart
        Job job3 = new Job(conf, "Req3");
        job3.setJarByClass(MRProjet.class);
        job3.setMapperClass(EventPurchasesMapper.class);
        job3.setReducerClass(EventPurchasesReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(INPUT_PATH + "datamart1.csv"));
        FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_PATH + "req3-" + Instant.now().getEpochSecond()));
        job3.waitForCompletion(true);

        // Job pour 1ere requete du datamart 2
        Job job4 = new Job(conf, "Req4");
        job4.setJarByClass(MRProjet.class);
        job4.setMapperClass(CharacterSessionsMapper.class);
        job4.setReducerClass(CharacterSessionsReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path(INPUT_PATH + "datamart2.csv"));
        FileOutputFormat.setOutputPath(job4, new Path(OUTPUT_PATH + "req4-" + Instant.now().getEpochSecond()));
        job4.waitForCompletion(true);

        // Job pour 2e requete du datamart 2
        Job job5 = new Job(conf, "Req5");
        job5.setJarByClass(MRProjet.class);
        job5.setMapperClass(VictoryRateMapper.class);
        job5.setReducerClass(VictoryRateReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5, new Path(INPUT_PATH + "datamart2.csv"));
        FileOutputFormat.setOutputPath(job5, new Path(OUTPUT_PATH + "req5-" + Instant.now().getEpochSecond()));
        job5.waitForCompletion(true);

    }
}
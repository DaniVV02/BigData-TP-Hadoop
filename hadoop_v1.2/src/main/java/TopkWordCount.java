
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/*
 * Jusqu'à présent nous avons défini nos mappers et reducers comme des classes internes à notre classe principale.
 * Dans des applications réelles de map-reduce cela ne sera généralement pas le cas, les classes seront probablement localisées dans d'autres fichiers.
 * Dans cet exemple, nous avons défini Map et Reduce en dehors de notre classe principale.
 * Il se pose alors le problème du passage du paramètre 'k' dans notre reducer, car il n'est en effet plus possible de déclarer un paramètre k dans notre classe principale qui serait partagé avec ses classes internes ; c'est la que la Configuration du Job entre en jeu.
 */

// =========================================================================
// MAPPER
// =========================================================================

/*
class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String[] words = line.split("\\s+");

		if (Arrays.equals(words, emptyWords))
			return;

		for (String word : words)
			context.write(new Text(word), one);
	}

}
 */

class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) return;

		String line = value.toString();
		String[] fields = line.split(",");

		// Exemple de champ dans le CSV : "clientId, profit, nomClient"
		if (fields.length > 20) {
			String clientId = fields[5];
			String profitStr = fields[20];
			try {
				double profit = Double.parseDouble(profitStr);
				context.write(new Text(clientId), new DoubleWritable(profit));
			} catch (NumberFormatException e) {
				System.err.println("Invalid number format for line: " + line);
			}
		}
	}
}

// =========================================================================
// REDUCER
// =========================================================================

//class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	/*
	private TreeMap<Integer, List<Text>> sortedWords = new TreeMap<>();
	private int nbsortedWords = 0;
	private int k;
	 */

	private TreeMap<Double, List<Text>> sortedProfits = new TreeMap<>();
	private int nbsortedProfits = 0;
	private int k;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		// k = context.getConfiguration().getInt("k", 1);
		k = context.getConfiguration().getInt("k", 10); // Récupère k
	}

	/*
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable val : values)
			sum += val.get();

		// On copie car l'objet key reste le même entre chaque appel du reducer
		Text keyCopy = new Text(key);

		// Fréquence déjà présente
		if (sortedWords.containsKey(sum))
			sortedWords.get(sum).add(keyCopy);
		else {
			List<Text> words = new ArrayList<>();

			words.add(keyCopy);
			sortedWords.put(sum, words);
		}

		// Nombre de mots enregistrés atteint : on supprime le mot le moins fréquent (le premier dans sortedWords)
		if (nbsortedWords == k) {
			Integer firstKey = sortedWords.firstKey();
			List<Text> words = sortedWords.get(firstKey);
			words.remove(words.size() - 1);

			if (words.isEmpty())
				sortedWords.remove(firstKey);
		} else
			nbsortedWords++;
	}
	 */

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double totalProfit = 0.0;

		for (DoubleWritable val : values) {
			totalProfit += val.get();
		}

		// Si la clé (clientId) est déjà présente dans le TreeMap, on ajoute le profit.
		// Sinon, on crée une nouvelle entrée.
		if (sortedProfits.containsKey(totalProfit)) {
			sortedProfits.get(totalProfit).add(new Text(key));
		} else {
			List<Text> clients = new ArrayList<>();
			clients.add(new Text(key));
			sortedProfits.put(totalProfit, clients);
		}

		// On garde uniquement les k premiers
		if (nbsortedProfits == k) {
			Double firstKey = sortedProfits.firstKey();
			List<Text> clients = sortedProfits.get(firstKey);
			clients.remove(clients.size() - 1); // On enlève le dernier client dans la liste

			if (clients.isEmpty()) {
				sortedProfits.remove(firstKey);
			}
		} else {
			nbsortedProfits++;
		}
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	/*
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Integer[] nbofs = sortedWords.keySet().toArray(new Integer[0]);

		// Parcours en sens inverse pour obtenir un ordre descendant
		int i = nbofs.length;

		while (i-- != 0) {
			Integer nbof = nbofs[i];

			for (Text words : sortedWords.get(nbof))
				context.write(words, new IntWritable(nbof));
		}
	}
	*/


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// On parcourt le TreeMap dans l'ordre descendant des profits
		Double[] profits = sortedProfits.keySet().toArray(new Double[0]);
		int i = profits.length;

		while (i-- != 0) {
			Double profit = profits[i];

			// Pour chaque client avec ce profit
			for (Text client : sortedProfits.get(profit)) {
				context.write(client, new DoubleWritable(profit));
			}
		}
	}
}

public class TopkWordCount {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/TopkExo10-";
	private static final Logger LOG = Logger.getLogger(TopkWordCount.class.getName());

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

	/**
	 * Ce programme permet le passage d'une valeur k en argument de la ligne de commande.
	 */
	public static void main(String[] args) throws Exception {
		// Borne 'k' du topk
		int k = 10;

		try {
			// Passage du k en argument ?
			if (args.length > 0) {
				k = Integer.parseInt(args[0]);

				// On contraint k à valoir au moins 1
				if (k <= 0) {
					LOG.warning("k must be at least 1, " + k + " given");
					k = 1;
				}
			}
		} catch (NumberFormatException e) {
			LOG.severe("Error for the k argument: " + e.getMessage());
			System.exit(1);
		}

		Configuration conf = new Configuration();
		conf.setInt("k", k);

		Job job = new Job(conf, "TopkProfit");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}
package SparkDemo.SparkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class WordCounter 
{
	private static void explainWordCount(String inputFile) throws InterruptedException 
	{
		Logger.getLogger("org.apache").setLevel(Level.INFO);
		
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("APT Word Counter").set("spark.executor.memory","2g");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputContent = sparkContext.textFile(inputFile, 4);
        //JavaRDD<String> inputfiltered = inputContent.map(s -> s.replaceAll("\\p{Punct}", " ").toLowerCase()); //Additional Step
        JavaRDD<String> wordsFromFile = inputContent.flatMap(content -> Arrays.asList(content.split(" ")).iterator()).filter(x -> x.trim().length() > 0);
        JavaPairRDD<String, Integer> mapData = wordsFromFile.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairRDD<String, Integer> countWords = mapData.reduceByKey((x, y) -> (int) x + (int) y);
        
        //countWords.foreach(data -> System.out.println(data._1() + " : " + data._2()));
        countWords.saveAsTextFile("Output/ExplainWordCount");
        Thread.sleep(180000);
        sparkContext.close();
    }
	
	private static void sparkWordCount(String inputFile, String outputFile) throws IOException, InterruptedException 
	{
		Logger.getLogger("org.apache").setLevel(Level.INFO);
		//.setMaster("spark://192.168.1.8:7077")
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").set("spark.executor.instances", "3").set("spark.executor.cores", "3").setAppName("APT Word Counter").setJars(new String[]{"/Users/sumanb/SB_Code_Dev/SparkDemo/target/SparkDemo-jar-with-dependencies.jar"});
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> lines = sparkContext.textFile(inputFile, 4);
        JavaRDD<String> words = lines.map(s -> s.replaceAll("\\p{Punct}", " ").toLowerCase()).flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(x -> x.trim().length() > 0);
        Map<String, Long> wordCount = words.countByValue();
        
        //wordCount.forEach((k,v) -> System.out.println(k + " : " + v));
        Thread.sleep(180000);
        sparkContext.close();
        
        TreeMap<String, Long> sorted = new TreeMap<>(); 
        sorted.putAll(wordCount);
        
        @SuppressWarnings("resource")
		BufferedWriter fileBuffer = new BufferedWriter( new FileWriter(new File(outputFile + "output.txt")) );
        sorted.forEach((k,v)-> {
			try 
			{
				fileBuffer.write(k + ":" + v + "\n");
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		});
	}
	
	private static void javaWordCount(String inputFile, String outputFile) throws IOException 
	{
		Instant start = Instant.now();
		
		String content = new String(Files.readAllBytes(Paths.get(inputFile)));
		List<String> words = Arrays.asList(content.replaceAll("\\p{Punct}"," ").toLowerCase().split(" ")).stream().filter(x -> x.trim().length() > 0).collect(Collectors.toList());
		
		Map<String, Long> wordCount = new HashMap<String, Long>();
		words.forEach(word ->
		{
			if (wordCount.containsKey(word)) 
			{
				wordCount.replace(word, wordCount.get(word) + 1);
			}
			else
			{
				wordCount.put(word, (long) 1);
			}
		});
		//wordCount.forEach((k,v)-> System.out.println(k + ":" + v));
		
		Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("Finished in : " + timeElapsed + " ms");
        
        TreeMap<String, Long> sorted = new TreeMap<>(); 
        sorted.putAll(wordCount);
        
        @SuppressWarnings("resource")
		BufferedWriter fileBuffer = new BufferedWriter( new FileWriter(new File(outputFile + "output.txt")) );
        sorted.forEach((k,v)-> {
			try 
			{
				fileBuffer.write(k + ":" + v + "\n");
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		});
	}

    public static void main(String[] args) throws IOException, InterruptedException 
    {
    	String option = "explain";
    	switch(option)
        {
    	case("spark"):
        	sparkWordCount("Input/input_big.txt", "Output/SparkWordCount/");
    		break;
    	case("java"):
        	javaWordCount("Input/input_big.txt", "Output/JavaWordCount/");
    		break;
    	case("explain"):
    		explainWordCount("Input/input.txt");
    		break;
    	default:
            System.out.println("No option provided.");
            System.exit(0);
        }
    }
}

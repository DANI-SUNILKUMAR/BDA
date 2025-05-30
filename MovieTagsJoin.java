import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieTagsJoin {

    // Mapper for Movies
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() == 0 && line.contains("movieId")) return; // skip header

            String[] fields = line.split(",", 3); // movieId,title,genres
            if (fields.length >= 2) {
                String movieId = fields[0].trim();
                String title = fields[1].trim();
                context.write(new Text(movieId), new Text("MOVIE::" + title));
            }
        }
    }

    // Mapper for Tags
    public static class TagMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() == 0 && line.contains("userId")) return; // skip header

            String[] fields = line.split(",", 4); // userId,movieId,tag,timestamp
            if (fields.length >= 3) {
                String movieId = fields[1].trim();
                String tag = fields[2].trim();
                context.write(new Text(movieId), new Text("TAG::" + tag));
            }
        }
    }

    // Reducer to join movies and tags
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = null;
            List<String> tags = new ArrayList<>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("MOVIE::")) {
                    movieTitle = value.substring(7);
                } else if (value.startsWith("TAG::")) {
                    tags.add(value.substring(5));
                }
            }

            if (movieTitle != null && !tags.isEmpty()) {
                context.write(new Text(movieTitle), new Text(String.join(", ", tags)));
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MovieTagsJoin <movies input> <tags input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Tags Join");

        job.setJarByClass(MovieTagsJoin.class);

        // Set Mappers
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TagMapper.class);

        job.setReducerClass(JoinReducer.class);

        // Output Types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job leagueJob = Job.getInstance(conf, "popular link");

        leagueJob.setMapperClass(LeagueMap.class);
        leagueJob.setMapOutputKeyClass(IntWritable.class);
        leagueJob.setMapOutputValueClass(IntWritable.class);

        leagueJob.setReducerClass(LeagueReduce.class);
        leagueJob.setOutputKeyClass(IntWritable.class);
        leagueJob.setOutputValueClass(IntWritable.class);
        leagueJob.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(leagueJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(leagueJob, new Path(args[1]));

//        leagueJob.setInputFormatClass(KeyValueTextInputFormat.class);
//        leagueJob.setOutputFormatClass(TextOutputFormat.class);

        leagueJob.setJarByClass(PopularityLeague.class);
        return leagueJob.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LeagueMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        List<String> leagueList;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            //set the path to league list
            String leagueListPath = conf.get("league");
            //get the league list: each line represents page id
            this.leagueList = Arrays.asList(readHDFSFile(leagueListPath, conf).split("\n"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");
            //set the current page to have 0 links
            write(new IntWritable(Integer.parseInt(line[0].trim())), new IntWritable(0), context);
            StringTokenizer tokenizer = new StringTokenizer(line[1], " \t");

            //set all the links in the current pages to have 1 link
            while (tokenizer.hasMoreTokens()) {
                write(new IntWritable(Integer.parseInt(tokenizer.nextToken().trim())), new IntWritable(1), context);
            }
        }

        public void write(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException{
            if(leagueList.contains(key.toString()))
                context.write(key, value);
        }
    }

    public static class LeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        TreeSet<Pair<Integer, Integer>> leagueWithCount = new TreeSet<Pair<Integer, Integer>>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            //sum up all the references
            for(IntWritable currentPage: values) {
                count+=currentPage.get();
            }
            leagueWithCount.add(new Pair<Integer, Integer>(count, key.get()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int prevIndex = -1;
            int prevCount = -1;
            int count =0;
            for (Pair<Integer, Integer> link : leagueWithCount) {
                if(link.first == prevCount) {
                    context.write(new IntWritable(link.second), new IntWritable(prevIndex));
                } else {
                    prevIndex =count;
                    prevCount = link.first;
                    context.write(new IntWritable(link.second), new IntWritable(count));
                }
                count++;
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}

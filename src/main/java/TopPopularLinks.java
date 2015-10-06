import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        //tmp path required to store intermediate data
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true); //discard the previous data

        //perform a count job
        Job linkCountJob = Job.getInstance(conf, "Link Count");

        linkCountJob.setMapperClass(LinkCountMap.class);
        linkCountJob.setMapOutputKeyClass(IntWritable.class);
        linkCountJob.setMapOutputValueClass(IntWritable.class);

        linkCountJob.setReducerClass(LinkCountReduce.class);
        linkCountJob.setOutputKeyClass(IntWritable.class);
        linkCountJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(linkCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(linkCountJob, tmpPath);

        linkCountJob.setJarByClass(TopPopularLinks.class);
        linkCountJob.waitForCompletion(true);

        //perform sorting job
        Job sortLinkCount = Job.getInstance(conf, "Sort Link Count");

        sortLinkCount.setMapperClass(TopLinksMap.class);
        sortLinkCount.setMapOutputKeyClass(NullWritable.class);
        sortLinkCount.setMapOutputValueClass(IntArrayWritable.class);

        sortLinkCount.setReducerClass(TopLinksReduce.class);
        sortLinkCount.setOutputKeyClass(IntWritable.class);
        sortLinkCount.setOutputValueClass(IntWritable.class);
        sortLinkCount.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(sortLinkCount, tmpPath);
        FileOutputFormat.setOutputPath(sortLinkCount, new Path(args[1]));

        sortLinkCount.setInputFormatClass(KeyValueTextInputFormat.class);
        sortLinkCount.setOutputFormatClass(TextOutputFormat.class);

        sortLinkCount.setJarByClass(TopPopularLinks.class);
        return sortLinkCount.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");
            //set the current page to have 0 links
            context.write(new IntWritable(Integer.parseInt(line[0].trim())), new IntWritable(0));
            StringTokenizer tokenizer = new StringTokenizer(line[1], " \t");

            //set all the links in the current pages to have 1 link
            while (tokenizer.hasMoreTokens()) {
                context.write(new IntWritable(Integer.parseInt(tokenizer.nextToken().trim())), new IntWritable(1));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            //sum up all the references
            for(IntWritable currentPage: values) {
                count+=currentPage.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        TreeSet<Pair<Integer, Integer>> linkCount = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer count = Integer.parseInt(value.toString());
            Integer link = Integer.parseInt(key.toString());

            linkCount.add(new Pair<Integer,Integer>(count, link));

            if (linkCount.size() > N) {
                linkCount.remove(linkCount.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> link : linkCount) {
                Integer[] linkWithCount = {link.second, link.first};
                IntArrayWritable val = new IntArrayWritable(linkWithCount);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        Integer N;
        TreeSet<Pair<Integer, Integer>> linkCount = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable val: values) {
                IntWritable[] pair= (IntWritable[]) val.toArray();

                Integer link = pair[0].get();
                Integer count = pair[1].get();

                linkCount.add(new Pair<Integer, Integer>(count, link));

                if (linkCount.size() > N) {
                    linkCount.remove(linkCount.first());
                }
            }

            for (Pair<Integer, Integer> linkWithCount: linkCount) {
                IntWritable link = new IntWritable(linkWithCount.second);
                IntWritable count = new IntWritable(linkWithCount.first);
                context.write(link, count);
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
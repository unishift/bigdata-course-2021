import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

public class candle {
    private static final ArrayList<String> COLUMNS_ORDER = new ArrayList<String>(Arrays.asList(
            "#SYMBOL", "SYSTEM", "MOMENT", "ID_DEAL", "PRICE_DEAL", "VOLUME", "OPEN_POS", "DIRECTION"
    ));

    public static class CandleKey implements WritableComparable<CandleKey> {
        public Text symbol;
        public Text moment;

        public CandleKey() {
            symbol = new Text();
            moment = new Text();
        }

        public CandleKey(String symbol, String moment) {
            this.symbol = new Text(symbol);
            this.moment = new Text(moment);
        }

        @Override
        public int compareTo(CandleKey candleKey) {
            int diff = symbol.compareTo(candleKey.symbol);
            if (diff != 0) return diff;

            return moment.compareTo(candleKey.moment);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            symbol.write(dataOutput);
            moment.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            symbol.readFields(dataInput);
            moment.readFields(dataInput);
        }

        @Override
        public String toString() {
            return symbol.toString() + "," + moment.toString();
        }

        @Override
        public int hashCode() {
            return symbol.toString().hashCode() + moment.toString().hashCode();
        }
    }

    public static class CandleValue implements Writable {
        public LongWritable left_id, right_id;
        public DoubleWritable open, high, low, close;

        CandleValue() {
            left_id = new LongWritable();
            right_id = new LongWritable();

            open = new DoubleWritable();
            high = new DoubleWritable();
            low = new DoubleWritable();
            close = new DoubleWritable();
        }

        CandleValue(long left_id, long right_id,
                double open, double high, double low, double close) {
            this.left_id = new LongWritable(left_id);
            this.right_id = new LongWritable(right_id);

            this.open = new DoubleWritable(open);
            this.high = new DoubleWritable(high);
            this.low = new DoubleWritable(low);
            this.close = new DoubleWritable(close);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            left_id.write(dataOutput);
            right_id.write(dataOutput);

            open.write(dataOutput);
            high.write(dataOutput);
            low.write(dataOutput);
            close.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            left_id.readFields(dataInput);
            right_id.readFields(dataInput);

            open.readFields(dataInput);
            high.readFields(dataInput);
            low.readFields(dataInput);
            close.readFields(dataInput);
        }

        @Override
        public String toString() {
            return String.format("%.1f,%.1f,%.1f,%.1f", open.get(), high.get(), low.get(), close.get());
        }
    }

    private static final SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static long getMomentFromDate(String str_date) throws IOException {
        Date date;
        try {
            date = date_format.parse(str_date);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
    }

    public static class CandleMapper
            extends Mapper<Object, Text, CandleKey, CandleValue>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String str_value = value.toString();

            // Check if header
            if (str_value.startsWith(COLUMNS_ORDER.get(0))) return;

            Configuration conf = context.getConfiguration();

            String[] cols = value.toString().split(",");

            // Prepare key
            String symbol = cols[0];
            String str_global_moment = cols[2];

            long global_moment = getMomentFromDate(str_global_moment);
            long candle_width = conf.getLong("candle.width", 300000);
            long candle_from = getMomentFromDate(conf.getLong("candle.date.from", 0L) + "0000000");
            long moment = ((global_moment - candle_from) / candle_width) * candle_width + candle_from;
            String str_moment = date_format.format(moment);

            // Prepare value
            long left_id = Long.parseLong(cols[3]);
            double open = Double.parseDouble(cols[4]);

            // Check borders
            Pattern p = Pattern.compile(conf.get("candle.securities"));
            if (!p.matcher(symbol).matches()) return;

            String left_time_border = conf.get("candle.date.from") + conf.get("candle.time.from") + "000";
            String right_time_border = conf.get("candle.date.to") + conf.get("candle.time.to") + "000";
            if (str_global_moment.compareTo(left_time_border) < 0 || str_global_moment.compareTo(right_time_border) >= 0)
                return;

            context.write(
                    new CandleKey(symbol, str_moment),
                    new CandleValue(left_id, left_id, open, open, open, open)
            );
        }
    }

    public static class CandleReducer
            extends Reducer<CandleKey, CandleValue, CandleKey, CandleValue> {
        MultipleOutputs<CandleKey, CandleValue> multipleOutputs;

        @Override
        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        public void reduce(CandleKey key, Iterable<CandleValue> values,
                           Context context
        ) throws IOException, InterruptedException {
            long left_id = Long.MAX_VALUE;
            long right_id = Long.MIN_VALUE;

            double open = 0.;
            double high = Double.MIN_VALUE;
            double low = Double.MAX_VALUE;
            double close = 0.;

            for (CandleValue old_value : values) {
                // Update open value
                if (old_value.left_id.get() < left_id) {
                    left_id = old_value.left_id.get();
                    open = old_value.open.get();
                }

                // Update close value
                if (old_value.right_id.get() > right_id) {
                    right_id = old_value.right_id.get();
                    close = old_value.close.get();
                }

                // Update high
                if (old_value.high.get() > high) high = old_value.high.get();

                // Update low
                if (old_value.low.get() < low) low = old_value.low.get();
            }

            CandleValue new_value = new CandleValue(left_id, right_id, open, high, low, close);
            if (context.getTaskAttemptID().getTaskID().isMap()) {
                context.write(key, new_value);
            } else {
                String path = key.symbol.toString();
                multipleOutputs.write("output", key, new_value, path);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    private static final SimpleDateFormat conf_date_format = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: candle <in> <out>");

            for (String s :
                    otherArgs) {
                System.out.println(s);
            }

            System.exit(2);
        }

        // Add default params
        conf.set("candle.width", conf.get("candle.width", "300000"));
        conf.set("candle.securities", conf.get("candle.securities", ".*"));
        conf.set("candle.date.from", conf.get("candle.date.from", "19000101"));
        conf.set("candle.date.to", conf.get("candle.date.to", "20200101"));
        conf.set("candle.time.from", conf.get("candle.time.from", "1000"));
        conf.set("candle.time.to", conf.get("candle.time.to", "1800"));
        conf.set("candle.num.reducers", conf.get("candle.num.reducers", "1"));

        conf.set("mapred.textoutputformat.separator", ",");

        // Fix date.to format to day before
        Date date_to = conf_date_format.parse(conf.get("candle.date.to"));
        date_to = new Date(date_to.getTime() - 24 * 60 * 60 * 1000L);
        conf.set("candle.date.to", conf_date_format.format(date_to));

        Job job = new Job(conf, "candle");
        job.setJarByClass(candle.class);
        job.setMapperClass(CandleMapper.class);
        job.setCombinerClass(CandleReducer.class);
        job.setReducerClass(CandleReducer.class);
        job.setOutputKeyClass(CandleKey.class);
        job.setOutputValueClass(CandleValue.class);

        job.setNumReduceTasks(Integer.parseInt(conf.get("candle.num.reducers")));

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(otherArgs[1]), true);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, CandleKey.class, CandleValue.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
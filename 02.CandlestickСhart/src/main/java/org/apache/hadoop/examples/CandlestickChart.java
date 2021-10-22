package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
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

public class CandlestickChart {
    private static ArrayList<String> COLUMNS_ORDER = new ArrayList<String>(Arrays.asList(
            "#SYMBOL", "SYSTEM", "MOMENT", "ID_DEAL", "PRICE_DEAL", "VOLUME", "OPEN_POS", "DIRECTION"
    ));

    public static class CandleKey implements WritableComparable<CandleKey> {
        public Text symbol;
        public LongWritable moment;

        public CandleKey() {
            symbol = new Text();
            moment = new LongWritable(0L);
        }

        public CandleKey(String symbol, long moment) {
            this.symbol = new Text(symbol);
            this.moment = new LongWritable(moment);
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
            return symbol.toString() + "," + date_format.format(moment.get());
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
            return open.toString() + "," + high.toString() + "," + low.toString() + "," + close.toString();
        }
    }

    private static SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMddhhmmssSSS");

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
            long global_moment = getMomentFromDate(cols[2]);
            long candle_width = conf.getLong("candle.width", 300000);
            long candle_from = conf.getLong("candle.from", 0L);
            long moment = ((global_moment - candle_from) / candle_width) * candle_width + candle_from;

            // Prepare value
            long left_id = Long.parseLong(cols[3]);
            double open = Double.parseDouble(cols[4]);

            // Check borders
            Pattern p = Pattern.compile(conf.get("candle.securities"));
            if (!p.matcher(symbol).matches()) return;

            long left_time_border = conf.getLong("candle.from", Long.MIN_VALUE);
            long right_time_border = conf.getLong("candle.to", Long.MAX_VALUE);
            if (global_moment < left_time_border || global_moment > right_time_border) return;

            context.write(
                new CandleKey(symbol, moment),
                new CandleValue(left_id, left_id, open, open, open, open)
            );
        }
    }

    public static class CandleReducer
            extends Reducer<CandleKey, CandleValue, CandleKey, CandleValue> {
        MultipleOutputs<CandleKey, CandleValue> multipleOutputs;

        @Override
        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs<CandleKey, CandleValue>(context);
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

    private static SimpleDateFormat conf_date_format = new SimpleDateFormat("yyyyMMddhhmm");
    private static long parseDate(String date, String time) throws ParseException {
        return conf_date_format.parse(date + time).getTime();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Add default params
        conf.setLong("candle.width", 300000);
        conf.set("candle.securities", ".*");
        conf.set("candle.date.from", "19000101");
        conf.set("candle.date.to", "20200101");
        conf.set("candle.time.from", "1000");
        conf.set("candle.time.to", "1800");
        conf.setLong("candle.num.reducers", 1L);

        conf.set("mapred.textoutputformat.separator", ",");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: candle <in> <out>");

            for (String s :
                    otherArgs) {
                System.out.println(s);
            }

            System.exit(2);
        }

        // Add precalculated values
        conf.setLong("candle.from", parseDate(conf.get("candle.date.from"), conf.get("candle.time.from")));
        conf.setLong("candle.to", parseDate(conf.get("candle.date.to"), conf.get("candle.time.to")));

        conf.set("candle.output", otherArgs[1]);

        Job job = new Job(conf, "candle");
        job.setJarByClass(CandlestickChart.class);
        job.setMapperClass(CandleMapper.class);
        job.setCombinerClass(CandleReducer.class);
        job.setReducerClass(CandleReducer.class);
        job.setOutputKeyClass(CandleKey.class);
        job.setOutputValueClass(CandleValue.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, CandleKey.class, CandleValue.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
package com.byjw.dataflow;

import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import com.byjw.nl.NLAnalyze;
import com.byjw.nl.NLAnalyzeVO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class TwitterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterPipeline.class);
    private static final String PROJECT = "beer-coding";
    private static final String NOUN_TABLE = "beer-coding:twitter.noun";
    private static final String ADJ_TABLE = "beer-coding:twitter.adj";
    private static final String TOPIC = "projects/beer-coding/topics/twitter";
    private static final String BUCKET = "gs://dataflow-byjw";



    public static void main(String[] args) {

        // No args settings
//        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
//        options.setRunner(DataflowPipelineRunner.class);
//        options.setProject(PROJECT);
//        options.setStagingLocation(BUCKET);
//        options.setStreaming(true);
//        Pipeline pipeline = Pipeline.create(options);

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        // pipeline.apply를 통해서 변형된 데이터를 PCollection에 담음)
        PCollection <KV<String,Iterable<String>>> nlProcessed =
                pipeline.apply(PubsubIO.Read.named("ReadFromPubSub").topic(TOPIC))
                        .apply(ParDo.named("Parse Twitter").of(new ParseTwitterFeedDoFn()))
                        .apply(ParDo.named("NL Processing").of(new NLAnalyticsDoFn()));

        // Noun handling sub-pipeline
        nlProcessed.apply(ParDo.named("NounFilter").of(new NounFilter()))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(Count.<String>perElement())
                .apply(ParDo.named("Noun Formating").of(new AddTimeStampNoun()) )
                .apply(BigQueryIO.Write
                        .named("Write Noun Count to BQ")
                        .to(NOUN_TABLE)
                        .withSchema(getNounSchema())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        // Adj handling sub-pipeline
        nlProcessed.apply(ParDo.named("AdjFilter").of(new AdjFilter()))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(Count.<String>perElement())
                .apply(ParDo.named("Adj Formating").of(new AddTimeStampAdj()) )
                .apply(BigQueryIO.Write
                        .named("Write Adj Count to BQ")
                        .to(ADJ_TABLE)
                        .withSchema(getAdjSchema())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }

    private static TableSchema getAdjSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("adj").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));

        return new TableSchema().setFields(fields);
    }

    private static TableSchema getNounSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("noun").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));

        return new TableSchema().setFields(fields);
    }

    // Read Twitter feed as a JSON format
    // extract twitt feed string and pass into next pipeline
    static class ParseTwitterFeedDoFn extends DoFn<String,String> {

        @Override
        public void processElement(ProcessContext processContext){
            String text;
            String lang;

            try {
                JSONObject jsonObject = new JSONObject(processContext.element());

                text = jsonObject.getString("text");
                lang = jsonObject.getString("lang");

                if (lang.equals("en")) {
                    processContext.output(text.toLowerCase());
                }

            }
            catch (Exception e) {
                LOG.debug("No text element");
                LOG.debug("original message is :" + processContext.element());
            }
        }
    }

    // Parse Twitter string into
    // - list of nouns
    // - list of adj
    // - list of emoticon

    static class NLAnalyticsDoFn extends DoFn< String, KV<String, Iterable<String>> > {

        // return list of NOUN, ADJ, Emoticon
        @Override
        public void processElement(ProcessContext processContext) throws IOException, GeneralSecurityException {
            String text = processContext.element();
            NLAnalyzeVO nlAnalyzeVO = NLAnalyze.getInstance().analyze(text);

            List<String> nouns = nlAnalyzeVO.getNouns();
            List<String> adjs = nlAnalyzeVO.getAdjs();

            KV < String, Iterable<String> > kv_noun=  KV.of("NOUN", (Iterable<String>)nouns);
            KV < String, Iterable<String> > kv_adj =  KV.of("ADJ", (Iterable<String>)adjs);

            processContext.output(kv_noun);
            processContext.output(kv_adj);
        }

    }


    static class NounFilter extends DoFn<KV<String,Iterable<String>>, String>{
        @Override
        public void processElement(ProcessContext processContext) {
            String key = processContext.element().getKey();
            if(!key.equals("NOUN")) return;
            List<String> values = (List<String>) processContext.element().getValue();
            for(String value:values){
                // Filtering #
                if(value.equals("#")) continue;
                else if(value.startsWith("http")) continue;
                processContext.output(value);
            }

        }
    }

    static class AddTimeStampNoun extends DoFn<KV<String,Long>,TableRow>
            implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
    {
        @Override
        public void processElement(ProcessContext processContext) {
            String key = processContext.element().getKey();	// get Word
            Long value = processContext.element().getValue();// get count of the word

            IntervalWindow intervalWindow = (IntervalWindow) processContext.window();
            Instant start = intervalWindow.start();

            DateTime startTime = start.toDateTime(org.joda.time.DateTimeZone.forID("Asia/Seoul"));
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

            String strStartTime = startTime.toString(dateTimeFormatter);

            TableRow row =  new TableRow()
                    .set("date", strStartTime)
                    .set("noun", key)
                    .set("count", value);

            processContext.output(row);
        }

    }

    static class AddTimeStampAdj extends DoFn<KV<String,Long>,TableRow>
            implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess {
        @Override
        public void processElement(ProcessContext processContext) {
            String key = processContext.element().getKey();	// get Word
            Long value = processContext.element().getValue();// get count of the word

            IntervalWindow intervalWindow = (IntervalWindow) processContext.window();
            Instant start = intervalWindow.start();

            DateTime startTime = start.toDateTime(org.joda.time.DateTimeZone.forID("Asia/Seoul"));
            DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

            String strStartTime = startTime.toString(dtf);

            TableRow row =  new TableRow()
                    .set("date", strStartTime)
                    .set("adj", key)
                    .set("count", value);

            processContext.output(row);
        }

    }

    static class AdjFilter extends DoFn<KV<String,Iterable<String>>,String> {
        @Override
        public void processElement(ProcessContext processContext) {
            String key = processContext.element().getKey();
            if(!key.equals("ADJ")) return;
            List<String> values = (List<String>) processContext.element().getValue();
            for(String value:values){
                processContext.output(value);
            }
        }
    }

    static class Echo extends DoFn<KV<String,Iterable<String>>,Void> {
        @Override
        public void processElement(ProcessContext processContext) {
            String key = processContext.element().getKey();
            List<String> values = (List<String>) processContext.element().getValue();
            for (String value:values){ }
        }

    }

}
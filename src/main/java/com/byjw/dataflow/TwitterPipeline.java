package com.byjw.dataflow;

import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import com.byjw.nl.NLAnalyze;
import com.byjw.nl.NLAnalyzeVO;
import com.google.api.services.bigquery.model.JsonObject;
import com.google.gson.stream.JsonReader;
import org.apache.avro.data.Json;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */

public class TwitterPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterPipeline.class);
    private static final String NOWN_TABLE= "beer-coding:twitter.noun";
    private static final String ADJ_TABLE= "beer-coding:twitter.adj";
    private static final String TOPIC = "projects/beer-coding/topics/twitter";


    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        PCollection <KV<String,Iterable<String>>> nlProcessed =
                pipeline.apply(PubsubIO.Read.named("ReadFromPubSub").topic(TOPIC))
                        .apply(ParDo.named("Parse Twitter").of(new ParseTwitterFeedDoFn()))
                        .apply(ParDo.named("NL Processing").of(new NLAnalyticsDoFn()));

        // Noun handling sub-pipeline
        TableSchema nounSchema = new TableSchema().setFields(getNounFields());

        nlProcessed.apply(ParDo.named("NounFilter").of(new NounFilter()))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(Count.<String>perElement())
                .apply(ParDo.named("Noun Formating").of(new AddTimeStampNoun()) )
                .apply(BigQueryIO.Write
                        .named("Write Noun Count to BQ")
                        .to(NOWN_TABLE)
                        .withSchema(nounSchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        // Adj handling sub-pipeline
        TableSchema adjSchema = new TableSchema().setFields(getAdjFields());

        nlProcessed.apply(ParDo.named("AdjFilter").of(new AdjFilter()))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(Count.<String>perElement())
                .apply(ParDo.named("Adj Formating").of(new AddTimeStampAdj()) )
                .apply(BigQueryIO.Write
                        .named("Write Adj Count to BQ")
                        .to(ADJ_TABLE)
                        .withSchema(adjSchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }

    private static List<TableFieldSchema> getAdjFields() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("adj").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
        return fields;
    }

    private static List<TableFieldSchema> getNounFields() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("noun").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));

        return fields;
    }

    // Read Twitter feed as a JSON format
    // extract twitt feed string and pass into next pipeline
    static class ParseTwitterFeedDoFn extends DoFn<String,String>{
        private static final long serialVersionUID = 3644510088969272245L;

        @Override
        public void processElement(ProcessContext c){
            String text = null;
            String lang = null;

            try {
                JsonReader jsonReader = Json.createReader(new StringReader(c.element()));
                JsonObject readObject = jsonReader.readObject();
                text = (String) readObject.getString("text");
                lang = (String) readObject.getString("lang");

                if(lang.equals("en")) {
                    c.output(text.toLowerCase());
                }

            }
            catch (Exception e) {
                LOG.debug("No text element");
                LOG.debug("original message is :" + c.element());
            }
        }
    }

    // Parse Twitter string into
    // - list of nouns
    // - list of adj
    // - list of emoticon

    static class NLAnalyticsDoFn extends DoFn<String,KV<String,Iterable<String>>> {
        private static final long serialVersionUID = 3013780586389810713L;

        // return list of NOUN,ADJ,Emoticon
        @Override
        public void processElement(ProcessContext processContext) throws IOException, GeneralSecurityException{
            String text = processContext.element();

            NLAnalyze nl = NLAnalyze.getInstance();
            NLAnalyzeVO vo = nl.analyze(text);

            List<String> nouns = vo.getNouns();
            List<String> adjs = vo.getAdjs();

            KV<String,Iterable<String>> kv_noun=  KV.of("NOUN", (Iterable<String>)nouns);
            KV<String,Iterable<String>> kv_adj =  KV.of("ADJ", (Iterable<String>)adjs);

            processContext.output(kv_noun);
            processContext.output(kv_adj);
        }

    }


    static class NounFilter extends DoFn<KV<String,Iterable<String>>,String>{
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
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();	// get Word
            Long value = c.element().getValue();// get count of the word
            IntervalWindow w = (IntervalWindow) c.window();
            Instant s = w.start();
            DateTime sTime = s.toDateTime(org.joda.time.DateTimeZone.forID("Asia/Seoul"));
            DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            String str_stime = sTime.toString(dtf);

            TableRow row =  new TableRow()
                    .set("date", str_stime)
                    .set("noun", key)
                    .set("count", value);

            c.output(row);
        }

    }

    static class AddTimeStampAdj extends DoFn<KV<String,Long>,TableRow>
            implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess
    {
        @Override
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();	// get Word
            Long value = c.element().getValue();// get count of the word
            IntervalWindow w = (IntervalWindow) c.window();
            Instant s = w.start();
            DateTime sTime = s.toDateTime(org.joda.time.DateTimeZone.forID("Asia/Seoul"));
            DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            String str_stime = sTime.toString(dtf);

            TableRow row =  new TableRow()
                    .set("date", str_stime)
                    .set("adj", key)
                    .set("count", value);

            c.output(row);
        }

    }
    static class AdjFilter extends DoFn<KV<String,Iterable<String>>,String>{
        @Override
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            if(!key.equals("ADJ")) return;
            List<String> values = (List<String>) c.element().getValue();
            for(String value:values){
                c.output(value);
            }
        }
    }

    static class Echo extends DoFn<KV<String,Iterable<String>>,Void>{
        @Override
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            List<String> values = (List<String>) c.element().getValue();
            for(String value:values){
            }
        }

    }


}
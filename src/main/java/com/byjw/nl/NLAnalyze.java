package com.byjw.nl;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPI;
import com.google.api.services.language.v1beta1.CloudNaturalLanguageAPIScopes;
import com.google.api.services.language.v1beta1.model.*;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

@SuppressWarnings("serial")
public class NLAnalyze {
    private final CloudNaturalLanguageAPI languageApi;
    private static final String APPLICATION_NAME = "Google-LanguagAPISample/1.0";

    private NLAnalyze(CloudNaturalLanguageAPI languageApi) {
        this.languageApi = languageApi;
    }

    public static NLAnalyze getInstance() throws IOException, GeneralSecurityException {
        return new NLAnalyze(getLanguageService());
    }

    public NLAnalyzeVO analyze(String text) throws IOException, GeneralSecurityException {
        Sentiment sentiment = analyzeSentiment(text);
        List<Token> tokens = analyzeSyntax(text);
        NLAnalyzeVO nlAnalyzeVO = new NLAnalyzeVO();

        for (Token token:tokens){
            String tag = token.getPartOfSpeech().getTag();
            String word = token.getText().getContent();

            if (tag.equals("NOUN")) nlAnalyzeVO.addNouns(word);
            else if(tag.equals("ADJ")) nlAnalyzeVO.addAdj(word);
        }

        nlAnalyzeVO.setSentimental(sentiment.getPolarity());

        return nlAnalyzeVO;
    }


    private static CloudNaturalLanguageAPI getLanguageService() throws IOException, GeneralSecurityException {
        GoogleCredential credential = GoogleCredential.getApplicationDefault().createScoped(CloudNaturalLanguageAPIScopes.all());
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        return getCloudNlBuilder(credential, jsonFactory);
    }


    private static CloudNaturalLanguageAPI getCloudNlBuilder(final GoogleCredential credential, JsonFactory jsonFactory) throws GeneralSecurityException, IOException {
        return new CloudNaturalLanguageAPI.Builder(GoogleNetHttpTransport.newTrustedTransport(), jsonFactory, new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest request) throws IOException {
                credential.initialize(request);
            }
        }).setApplicationName(APPLICATION_NAME).build();
    }


    private List<Token> analyzeSyntax(String text) throws IOException{
        AnnotateTextRequest request =
                new AnnotateTextRequest()
                        .setDocument(new Document().setContent(text).setType("PLAIN_TEXT"))
                        .setFeatures(new Features().setExtractSyntax(true))
                        .setEncodingType("UTF16");

        CloudNaturalLanguageAPI.Documents.AnnotateText analyze =
                languageApi.documents().annotateText(request);

        AnnotateTextResponse response = analyze.execute();

        return response.getTokens();
    }


    private Sentiment analyzeSentiment(String text) throws IOException {
        AnalyzeSentimentRequest request =
                new AnalyzeSentimentRequest().setDocument(new Document().setContent(text).setType("PLAIN_TEXT"));

        CloudNaturalLanguageAPI.Documents.AnalyzeSentiment analyze = languageApi.documents().analyzeSentiment(request);

        AnalyzeSentimentResponse response = analyze.execute();
        return response.getDocumentSentiment();
    }

}
package com.byjw.NL;

import com.google.cloud.language.v1beta2.*;

import java.io.IOException;
import java.util.List;

public class NLAnalyzeV2 {

    private static NLAnalyzeV2 instance = new NLAnalyzeV2();

    public static NLAnalyzeV2 getInstance() {
        return instance;
    }

    public void analyze(String text) {
        try (LanguageServiceClient languageServiceClient = LanguageServiceClient.create()) {
            Document document = Document.newBuilder().setContent(text).setType(Document.Type.PLAIN_TEXT).build();

            List<Token> tokenList = analyzeSyntax(languageServiceClient, document);
            Sentiment sentiment = analyzeSentiment(languageServiceClient, document);
            List<Entity> entityList = analyzeEntities(languageServiceClient, document);


            for (Token token : tokenList) {
                System.out.println(token.getText());
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private List<Token> analyzeSyntax(LanguageServiceClient languageServiceClient, Document document) throws IOException {

        AnalyzeSyntaxRequest request = AnalyzeSyntaxRequest.newBuilder()
                .setDocument(document)
                .setEncodingType(EncodingType.UTF16)
                .build();

        AnalyzeSyntaxResponse response = languageServiceClient.analyzeSyntax(request);

        return response.getTokensList();
    }


    private Sentiment analyzeSentiment(LanguageServiceClient languageServiceClient, Document document) throws IOException {

        AnalyzeSentimentResponse response = languageServiceClient.analyzeSentiment(document);
        return response.getDocumentSentiment();
    }


    private List<Entity> analyzeEntities(LanguageServiceClient languageServiceClient, Document document) throws IOException {

        AnalyzeEntitiesRequest request = AnalyzeEntitiesRequest.newBuilder()
                .setDocument(document)
                .setEncodingType(EncodingType.UTF16)
                .build();

        AnalyzeEntitiesResponse response = languageServiceClient.analyzeEntities(request);

        return response.getEntitiesList();
    }

}

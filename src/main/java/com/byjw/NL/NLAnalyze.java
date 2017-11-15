package com.byjw.NL;

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

/**
 * com.google.cloud.language.v1beta1 버전
 */

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

        for (Token token:tokens) {
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

    /*
        구문을 분석하여, 명사,형용사,접속사,조사 등과 단어간의 의존 관계등을 분석해서 리턴해주는데, Token 이라는 데이타 타입의 리스트 형태로 다음과 같이 리턴한다.
        List <Token> tokens = analyzeSyntax(text);

        여기서 단어의 형(명사,형용사)는 token에서 tag 라는 필드를 통해서 리턴되는데, 우리가 필요한것은 명사와 형용사만 필요하기 때문에,
        tag가 NOUN (명사)와 ADJ (형용사)로 된 단어만 추출해서 NLAnalyzeVO 객체에 넣어서 리턴한다.
        (태그의 종류는 https://cloud.google.com/natural-language/reference/rest/v1beta1/documents/annotateText#Tag ) 를 참고하기 바란다.

     */
    private List<Token> analyzeSyntax(String text) throws IOException {
        AnnotateTextRequest request =
                new AnnotateTextRequest()
                        .setDocument(new Document().setContent(text).setType("PLAIN_TEXT"))
                        .setFeatures(new Features().setExtractSyntax(true))
                        .setEncodingType("UTF16");

        CloudNaturalLanguageAPI.Documents.AnnotateText analyze = languageApi.documents().annotateText(request);
        AnnotateTextResponse response = analyze.execute();

        return response.getTokens();
    }


    private Sentiment analyzeSentiment(String text) throws IOException {
        AnalyzeSentimentRequest request =
                new AnalyzeSentimentRequest()
                        .setDocument(new Document().setContent(text).setType("PLAIN_TEXT"));

        CloudNaturalLanguageAPI.Documents.AnalyzeSentiment analyze = languageApi.documents().analyzeSentiment(request);
        AnalyzeSentimentResponse response = analyze.execute();

        return response.getDocumentSentiment();
    }

}
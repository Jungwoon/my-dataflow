package com.example;


import com.byjw.NL.NLAnalyze;
import com.byjw.NL.NLAnalyzeV2;
import com.byjw.NL.NLAnalyzeVO;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;


import static org.junit.Assert.fail;

public class NLAnalyzeTest {

    @Ignore
    @Test
    public void dataflow_nl_test() {

        try {
            NLAnalyze instance = NLAnalyze.getInstance();
            String text="inspects the given text for known entities (proper nouns and common nouns), returns information about those entities, and identifies the prevailing emotional opinion of the entity within the text, especially to determine a writer's attitude toward the entity as positive, negative, or neutral. Entity analysis is performed with the analyzeEntitySentiment method.";

            NLAnalyzeVO vo = instance.analyze(text);

            List<String> nouns = vo.getNouns();
            List<String> adjs = vo.getAdjs();

            System.out.println("### NOUNS");

            for(String noun:nouns){
                System.out.println(noun);
            }


            System.out.println("\n### ADJS");

            for(String adj:adjs){
                System.out.println(adj);
            }

        }
        catch (IOException e) {
            e.printStackTrace();
            fail("API call error");
        }
        catch (GeneralSecurityException e) {
            e.printStackTrace();
            fail("Security exception");
        }
    }

    @Ignore
    @Test
    public void parse_json() {

        String Sample = "{\"created_at\":\"Wed Nov 15 05:03:18 +0000 2017\",\"id\":930662470758420482,\"id_str\":\"930662470758420482\",\"text\":\"RT @Apple_believer_: 「✨iphone X✨」\\n\\n速報！iphoneX入荷しました！！\\n\\n気になる応募方法はRTorFollowのみ✨\\n\\nさらに今までに当選経験がない方には当選確率UP\uD83D\uDD3A\uD83D\uDD3A\\n\\nフォロワーが少ないうちは当選確率がさらに上がります(^^)… \",\"source\":\"<a href=\\\"http://twitter.com/download/iphone\\\" rel=\\\"nofollow\\\">Twitter for iPhone</a>\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":1030859467,\"id_str\":\"1030859467\",\"name\":\"リノ\",\"screen_name\":\"smoker_mb\",\"location\":\"奈良 近畿地方\",\"url\":null,\"description\":\"ぽっちゃり系が大好き(*´ω｀*)\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":59,\"friends_count\":676,\"listed_count\":0,\"favourites_count\":739,\"statuses_count\":42,\"created_at\":\"Sun Dec 23 16:07:56 +0000 2012\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"lang\":\"ja\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http://abs.twimg.com/images/themes/theme1/bg.png\",\"profile_background_image_url_https\":\"https://abs.twimg.com/images/themes/theme1/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"1DA1F2\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http://pbs.twimg.com/profile_images/764024410621698048/lLMKhGVf_normal.jpg\",\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/764024410621698048/lLMKhGVf_normal.jpg\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null}}";
        String text;

        try {

            JSONObject obj = new JSONObject(Sample);
            text = obj.getString("text");

            System.out.print("\nJSON Parsing : " + text);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("No text element");
            fail("original message is :");
        }
    }

    @Test
    public void nl_test() {

        String text = "inspects the given text for known entities (proper nouns and common nouns), returns information about those entities, and identifies the prevailing emotional opinion of the entity within the text, especially to determine a writer's attitude toward the entity as positive, negative, or neutral. Entity analysis is performed with the analyzeEntitySentiment method.";

        try {
            NLAnalyzeV2.getInstance().analyze(text);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


}

package org.dataone.hashstore.testdata;

import java.util.HashMap;
import java.util.Map;

/*
 * This class returns the test data expected hex digest values
 */
public class TestDataHarness {
    public Map<String, Map<String, String>> pidData;
    public String[] pidList = { "doi:10.18739/A2901ZH2M", "jtao.1700.1",
            "urn:uuid:1b35d0a5-b17a-423b-a2ed-de2b18dc367a" };

    public TestDataHarness() {
        Map<String, Map<String, String>> pidsAndHexDigests = new HashMap<>();

        Map<String, String> values1 = new HashMap<>();
        values1.put("s_cid", "0d555ed77052d7e166017f779cbc193357c3a5006ee8b8457230bcf7abcef65e");
        values1.put("md2", "b33c730ac5e36b2b886a9cd14552f42e");
        values1.put("md5", "db91c910a3202478c8def1071c54aae5");
        values1.put("sha1", "1fe86e3c8043afa4c70857ca983d740ad8501ccd");
        values1.put("sha256", "4d198171eef969d553d4c9537b1811a7b078f9a3804fc978a761bc014c05972c");
        values1.put("sha384",
                "d5953bd802fa74edea72eb941ead7a27639e62792fedc065d6c81de6c613b5b8739ab1f90e7f24a7500d154a727ed7c2");
        values1.put("sha512",
                "e9bcd6b91b102ef5803d1bd60c7a5d2dbec1a2baf5f62f7da60de07607ad6797d6a9b740d97a257fd2774f2c26503d455d8f2a03a128773477dfa96ab96a2e54");
        pidsAndHexDigests.put("doi:10.18739/A2901ZH2M", values1);

        Map<String, String> values2 = new HashMap<>();
        values2.put("s_cid", "a8241925740d5dcd719596639e780e0a090c9d55a5d0372b0eaf55ed711d4edf");
        values2.put("md2", "9c25df1c8ba1d2e57bb3fd4785878b85");
        values2.put("md5", "f4ea2d07db950873462a064937197b0f");
        values2.put("sha1", "3d25436c4490b08a2646e283dada5c60e5c0539d");
        values2.put("sha256", "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        values2.put("sha384",
                "a204678330fcdc04980c9327d4e5daf01ab7541e8a351d49a7e9c5005439dce749ada39c4c35f573dd7d307cca11bea8");
        values2.put("sha512",
                "bf9e7f4d4e66bd082817d87659d1d57c2220c376cd032ed97cadd481cf40d78dd479cbed14d34d98bae8cebc603b40c633d088751f07155a94468aa59e2ad109");
        pidsAndHexDigests.put("jtao.1700.1", values2);

        Map<String, String> values3 = new HashMap<>();
        values3.put("s_cid", "7f5cc18f0b04e812a3b4c8f686ce34e6fec558804bf61e54b176742a7f6368d6");
        values3.put("md2", "9f2b06b300f661ce4398006c41d8aa88");
        values3.put("md5", "e1932fc75ca94de8b64f1d73dc898079");
        values3.put("sha1", "c6d2a69a3f5adaf478ba796c114f57b990cf7ad1");
        values3.put("sha256", "4473516a592209cbcd3a7ba4edeebbdb374ee8e4a49d19896fafb8f278dc25fa");
        values3.put("sha384",
                "b1023a9be5aa23a102be9bce66e71f1f1c7a6b6b03e3fc603e9cd36b4265671e94f9cc5ce3786879740536994489bc26");
        values3.put("sha512",
                "c7fac7e8aacde8546ddb44c640ad127df82830bba6794aea9952f737c13a81d69095865ab3018ed2a807bf9222f80657faf31cfde6c853d7b91e617e148fec76");
        pidsAndHexDigests.put("urn:uuid:1b35d0a5-b17a-423b-a2ed-de2b18dc367a", values3);

        this.pidData = pidsAndHexDigests;
    }
}

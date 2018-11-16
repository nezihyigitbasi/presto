/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.JsonCodec;
import io.airlift.json.ObjectMapperProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.openjdk.jmh.annotations.Level.Invocation;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(3)
@Warmup(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkAfterBurner
{
    private static final JsonCodec<TaskUpdateRequest> CODEC = jsonCodec(TaskUpdateRequest.class);
//    Content-Length: 13880
    private static final String taskUpdateRequestJson = "{\"session\":{\"queryId\":\"20181114_223547_54089_wputn\",\"transactionId\":\"a673600f-1b8e-41de-b23f-9ee2b0504f03\",\"clientTransactionSupport\":true,\"user\":\"grp:ads_interfaces_pipelines,ztang\",\"principal\":\"CN=svc:chronos_secgrp_ads_interfaces_pipelines\",\"source\":\"dataswarm@fastlane-worker@ad_reporting.common_data_platform.crm_optimal_solutions.ad_adtable_ind_hourly.load[ad_adtable_ind_hourly]:2\",\"catalog\":\"prism_batch\",\"schema\":\"ad_reporting\",\"path\":{},\"timeZoneKey\":1825,\"locale\":\"en_US\",\"remoteUserAddress\":\"2401:db00:1120:20f5:face:0:7:0\",\"userAgent\":\"python-requests/2.20.0\",\"clientInfo\":\"{\\\"delegationToken\\\":{\\\"project\\\":\\\"ad_reporting.common_data_platform.crm_optimal_solutions.ad_adtable_ind_hourly\\\"},\\\"resource_use_case\\\":\\\"PRODUCTION_UNSTABLE\\\",\\\"isProductionCode\\\":\\\"1\\\",\\\"runMode\\\":\\\"worker\\\",\\\"periodId\\\":\\\"1542229200\\\",\\\"chronosJobInstanceRunCount\\\":1,\\\"operator\\\":\\\"PrestoInsertOperator\\\",\\\"taskFile\\\":\\\"\\\\/var\\\\/facebook\\\\/data\\\\/dataswarm\\\\/tasks\\\\/ad_reporting\\\\/common_data_platform\\\\/crm_optimal_solutions\\\\/ad_adtable_ind_hourly.py\\\",\\\"namespace\\\":\\\"ad_reporting\\\",\\\"owner\\\":\\\"ztang\\\",\\\"dataswarmPatchDiffId\\\":\\\"\\\",\\\"dateId\\\":\\\"2018-11-14\\\",\\\"chronosJobName\\\":\\\"DATASWARM.ad_reporting.common_data_platform.crm_optimal_solutions.ad_adtable_ind_hourly.load[ad_adtable_ind_hourly]\\\",\\\"dataswarmConfig\\\":\\\"dataswarm\\\",\\\"taskName\\\":\\\"load[ad_adtable_ind_hourly]\\\",\\\"type\\\":\\\"DATASWARM\\\",\\\"operator_id\\\":\\\"b4f8bcd9-31b5-4828-9d07-4514fa26f4c1\\\",\\\"schedule\\\":\\\"@hourly\\\",\\\"oncallId\\\":143438366346673,\\\"pipelineName\\\":\\\"ad_reporting.common_data_platform.crm_optimal_solutions.ad_adtable_ind_hourly\\\",\\\"oncall\\\":\\\"ads_common_data_platform\\\",\\\"chronosJobInstanceId\\\":\\\"7121400032\\\",\\\"_PrestoInsertOperatorTemplateMD5\\\":\\\"bb45df4fca5108bf1f7186f54710f0e5\\\",\\\"isKDS\\\":\\\"True\\\",\\\"language_version\\\":\\\"2.7.11\\\",\\\"language\\\":\\\"python\\\",\\\"chronosCluster\\\":\\\"atn\\\",\\\"project\\\":\\\"ad_reporting.common_data_platform.crm_optimal_solutions.ad_adtable_ind_hourly\\\",\\\"secureGroup\\\":\\\"ads_interfaces_pipelines\\\",\\\"presto_client\\\":\\\"_GenericPrestoClient\\\",\\\"sla\\\":\\\"FAST_LANE\\\"}\",\"clientTags\":[\"ad_reporting\",\"FAST_LANE\",\"PRODUCTION_UNSTABLE\"],\"clientCapabilities\":[],\"resourceEstimates\":{\"executionTime\":\"4302788.00ms\",\"cpuTime\":\"642030190.00ms\",\"peakMemory\":\"0B\"},\"startTime\":1542234947114,\"systemProperties\":{\"query_priority\":\"91\",\"query_max_memory\":\"5.5TB\"},\"catalogProperties\":{\"prism_batch\":{\"collect_column_statistics_on_write\":\"true\",\"orc_optimized_writer_max_stripe_rows\":\"20000000\",\"insert_existing_partitions_behavior\":\"OVERWRITE\",\"orc_optimized_writer_max_stripe_size\":\"128MB\",\"orc_optimized_writer_min_stripe_size\":\"104MB\",\"orc_optimized_writer_max_dictionary_memory\":\"32MB\",\"orc_optimized_writer_validate_percentage\":\"0.0\",\"orc_optimized_writer_validate\":\"false\",\"dwrf_aligned_write_enabled\":\"true\"}},\"preparedStatements\":{}, \"unprocessedCatalogProperties\":{}},\"sources\":[],\"outputIds\":{\"type\":\"ARBITRARY\",\"version\":385,\"noMoreBufferIds\":false,\"buffers\":{\"0\":0,\"1\":1,\"2\":2,\"3\":3,\"4\":4,\"5\":5,\"6\":6,\"7\":7,\"8\":8,\"9\":9,\"10\":10,\"11\":11,\"12\":12,\"13\":13,\"14\":14,\"15\":15,\"16\":16,\"17\":17,\"18\":18,\"19\":19,\"20\":20,\"21\":21,\"22\":22,\"23\":23,\"24\":24,\"25\":25,\"26\":26,\"27\":27,\"28\":28,\"29\":29,\"30\":30,\"31\":31,\"32\":32,\"33\":33,\"34\":34,\"35\":35,\"36\":36,\"37\":37,\"38\":38,\"39\":39,\"40\":40,\"41\":41,\"42\":42,\"43\":43,\"44\":44,\"45\":45,\"46\":46,\"47\":47,\"48\":48,\"49\":49,\"50\":50,\"51\":51,\"52\":52,\"53\":53,\"54\":54,\"55\":55,\"56\":56,\"57\":57,\"58\":58,\"59\":59,\"60\":60,\"61\":61,\"62\":62,\"63\":63,\"64\":64,\"65\":65,\"66\":66,\"67\":67,\"68\":68,\"69\":69,\"70\":70,\"71\":71,\"72\":72,\"73\":73,\"74\":74,\"75\":75,\"76\":76,\"77\":77,\"78\":78,\"79\":79,\"80\":80,\"81\":81,\"82\":82,\"83\":83,\"84\":84,\"85\":85,\"86\":86,\"87\":87,\"88\":88,\"89\":89,\"90\":90,\"91\":91,\"92\":92,\"93\":93,\"94\":94,\"95\":95,\"96\":96,\"97\":97,\"98\":98,\"99\":99,\"100\":100,\"101\":101,\"102\":102,\"103\":103,\"104\":104,\"105\":105,\"106\":106,\"107\":107,\"108\":108,\"109\":109,\"110\":110,\"111\":111,\"112\":112,\"113\":113,\"114\":114,\"115\":115,\"116\":116,\"117\":117,\"118\":118,\"119\":119,\"120\":120,\"121\":121,\"122\":122,\"123\":123,\"124\":124,\"125\":125,\"126\":126,\"127\":127,\"128\":128,\"129\":129,\"130\":130,\"131\":131,\"132\":132,\"133\":133,\"134\":134,\"135\":135,\"136\":136,\"137\":137,\"138\":138,\"139\":139,\"140\":140,\"141\":141,\"142\":142,\"143\":143,\"144\":144,\"145\":145,\"146\":146,\"147\":147,\"148\":148,\"149\":149,\"150\":150,\"151\":151,\"152\":152,\"153\":153,\"154\":154,\"155\":155,\"156\":156,\"157\":157,\"158\":158,\"159\":159,\"160\":160,\"161\":161,\"162\":162,\"163\":163,\"164\":164,\"165\":165,\"166\":166,\"167\":167,\"168\":168,\"169\":169,\"170\":170,\"171\":171,\"172\":172,\"173\":173,\"174\":174,\"175\":175,\"176\":176,\"177\":177,\"178\":178,\"179\":179,\"180\":180,\"181\":181,\"182\":182,\"183\":183,\"184\":184,\"185\":185,\"186\":186,\"187\":187,\"188\":188,\"189\":189,\"190\":190,\"191\":191,\"192\":192,\"193\":193,\"194\":194,\"195\":195,\"196\":196,\"197\":197,\"198\":198,\"199\":199,\"200\":200,\"201\":201,\"202\":202,\"203\":203,\"204\":204,\"205\":205,\"206\":206,\"207\":207,\"208\":208,\"209\":209,\"210\":210,\"211\":211,\"212\":212,\"213\":213,\"214\":214,\"215\":215,\"216\":216,\"217\":217,\"218\":218,\"219\":219,\"220\":220,\"221\":221,\"222\":222,\"223\":223,\"224\":224,\"225\":225,\"226\":226,\"227\":227,\"228\":228,\"229\":229,\"230\":230,\"231\":231,\"232\":232,\"233\":233,\"234\":234,\"235\":235,\"236\":236,\"237\":237,\"238\":238,\"239\":239,\"240\":240,\"241\":241,\"242\":242,\"243\":243,\"244\":244,\"245\":245,\"246\":246,\"247\":247,\"248\":248,\"249\":249,\"250\":250,\"251\":251,\"252\":252,\"253\":253,\"254\":254,\"255\":255,\"256\":256,\"257\":257,\"258\":258,\"259\":259,\"260\":260,\"261\":261,\"262\":262,\"263\":263,\"264\":264,\"265\":265,\"266\":266,\"267\":267,\"268\":268,\"269\":269,\"270\":270,\"271\":271,\"272\":272,\"273\":273,\"274\":274,\"275\":275,\"276\":276,\"277\":277,\"278\":278,\"279\":279,\"280\":280,\"281\":281,\"282\":282,\"283\":283,\"284\":284,\"285\":285,\"286\":286,\"287\":287,\"288\":288,\"289\":289,\"290\":290,\"291\":291,\"292\":292,\"293\":293,\"294\":294,\"295\":295,\"296\":296,\"297\":297,\"298\":298,\"299\":299,\"300\":300,\"301\":301,\"302\":302,\"303\":303,\"304\":304,\"305\":305,\"306\":306,\"307\":307,\"308\":308,\"309\":309,\"310\":310,\"311\":311,\"312\":312,\"313\":313,\"314\":314,\"315\":315,\"316\":316,\"317\":317,\"318\":318,\"319\":319,\"320\":320,\"321\":321,\"322\":322,\"323\":323,\"324\":324,\"325\":325,\"326\":326,\"327\":327,\"328\":328,\"329\":329,\"330\":330,\"331\":331,\"332\":332,\"333\":333,\"334\":334,\"335\":335,\"336\":336,\"337\":337,\"338\":338,\"339\":339,\"340\":340,\"341\":341,\"342\":342,\"343\":343,\"344\":344,\"345\":345,\"346\":346,\"347\":347,\"348\":348,\"349\":349,\"350\":350,\"351\":351,\"352\":352,\"353\":353,\"354\":354,\"355\":355,\"356\":356,\"357\":357,\"358\":358,\"359\":359,\"360\":360,\"361\":361,\"362\":362,\"363\":363,\"364\":364,\"365\":365,\"366\":366,\"367\":367,\"368\":368,\"369\":369,\"370\":370,\"371\":371,\"372\":372,\"373\":373,\"374\":374,\"375\":375,\"376\":376,\"377\":377,\"378\":378,\"379\":379,\"380\":380,\"381\":381,\"382\":382,\"383\":383,\"384\":384}}}";

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"false", "true"})
        private boolean codeGenEnabled;

        private TaskUpdateRequest request;
        private InputStream inputStream;
        private ObjectMapper objectMapper;

        @Setup(Invocation)
        public void setup()
        {
            request = CODEC.fromJson(taskUpdateRequestJson);
            inputStream = new ByteArrayInputStream(CODEC.toJsonBytes(request));
            objectMapper = new ObjectMapperProvider().get();
        }
    }

    @Benchmark
    public void deSerializeTaskUpdateRequest(BenchmarkData data, Blackhole blackhole)
            throws IOException
    {
        ObjectMapper objectMapper = data.objectMapper;

        JsonParser jsonParser = objectMapper.getFactory().createParser(data.inputStream);

        // Important: we are NOT to close the underlying stream after
        // mapping, so we need to instruct parser:
        jsonParser.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

        blackhole.consume(objectMapper.readValue(jsonParser, objectMapper.getTypeFactory().constructType(TaskUpdateRequest.class)));
    }

    @Benchmark
    public void serializeTaskUpdateRequest(BenchmarkData data, Blackhole blackhole)
    {
        blackhole.consume(CODEC.toJsonBytes(data.request));
    }

    public static void main(String[] args)
            throws Throwable
    {
//        BenchmarkData data = new BenchmarkData();
//        data.setup();
//
//        new BenchmarkAfterBurner().deSerializeTaskUpdateRequest(data, new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));

        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkAfterBurner.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}

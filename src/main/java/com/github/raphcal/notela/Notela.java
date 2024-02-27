package com.github.raphcal.notela;

import com.github.raphcal.localserver.HttpConstants;
import com.github.raphcal.localserver.HttpRequest;
import com.github.raphcal.localserver.HttpResponse;
import com.github.raphcal.localserver.HttpServlet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import fr.bdf.center.graalod.api.elastic.mock.BulkResponse;
import fr.bdf.center.graalod.api.elastic.mock.BulkResponseItem;
import fr.bdf.center.graalod.api.elastic.mock.CreateResponse;
import fr.bdf.center.graalod.api.elastic.mock.FieldMapping;
import fr.bdf.center.graalod.api.elastic.mock.GetResponse;
import fr.bdf.center.graalod.api.elastic.mock.LongSearchResponseHitsTotal;
import fr.bdf.center.graalod.api.elastic.mock.MultiSearchResponse;
import fr.bdf.center.graalod.api.elastic.mock.RelationSearchResponseHitsTotal;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponse;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponseHit;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponseHits;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponseHitsTotal;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponseHitsTotalRelation;
import fr.bdf.center.graalod.api.elastic.mock.SearchResponseHitsTotalSerializer;
import fr.bdf.center.graalod.api.elastic.mock.Shards;
import fr.bdf.center.graalod.api.elastic.mock.TermsBucket;
import fr.bdf.center.graalod.api.elastic.mock.VersionNumber;
import fr.bdf.center.graalod.api.elastic.painless.Painless;
import fr.bdf.center.graalod.util.SizedMap;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.github.raphcal.logdorak.Logger;
import java.util.Arrays;
import java.util.regex.Matcher;


/**
 * Mock d'Elasticsearch
 *
 * @author Raphaël Calabro (raphael.calabro.external2@banque-france.fr)
 */
@SuppressWarnings("unchecked")
public class Notela extends HttpServlet {

    /**
     * Journalisation.
     */
    private static final Logger LOGGER = new Logger(Notela.class);

    /**
     * Type MIME pour le JSON.
     */
    private static final String JSON_CONTENT_TYPE = "application/json";

    private static final Query NO_QUERY = object -> 1.0;

    private static final Highlighter NO_HIGHLIGHTER  = object -> Collections.emptyMap();

    private VersionNumber version = new VersionNumber("7.0.0");

    private Gson gson = new GsonBuilder()
            .serializeNulls()
            .registerTypeAdapter(SearchResponseHitsTotal.class, new SearchResponseHitsTotalSerializer())
            .create();
    private final Map<String, Scroll> scrolls = new SizedMap<>(20);

    private Map<String, Index> indexes;

    @Override
    public void doGet(HttpRequest request, HttpResponse response) {
        response.setContentType(JSON_CONTENT_TYPE);
        response.setCharset(StandardCharsets.UTF_8);
        response.setContent("{\"error\":\"Incorrect HTTP method for uri [" + request.getTarget() + "] and method [GET]\",\"status\":405}");
        try {
            String target = request.getTarget();
            final HashMap<String, String> queryParameters = new HashMap<>();
            target = parseQueryParameters(target, queryParameters);
            if ("/".equals(target)) {
                response.setContent("{\n"
                        + "  \"name\" : \"VD211810\",\n"
                        + "  \"cluster_name\" : \"elasticsearch\",\n"
                        + "  \"cluster_uuid\" : \"MQUTNBCOQbCbHNM0BeFKBg\",\n"
                        + "  \"version\" : {\n"
                        + "    \"number\" : \"" + version + "\",\n"
                        + "    \"build_flavor\" : \"default\",\n"
                        + "    \"build_type\" : \"zip\",\n"
                        + "    \"build_hash\" : \"b7e28a7\",\n"
                        + "    \"build_date\" : \"2019-04-05T22:55:32.697037Z\",\n"
                        + "    \"build_snapshot\" : false,\n"
                        + "    \"lucene_version\" : \"8.0.0\",\n"
                        + "    \"minimum_wire_compatibility_version\" : \"6.7.0\",\n"
                        + "    \"minimum_index_compatibility_version\" : \"6.0.0-beta1\"\n"
                        + "  },\n"
                        + "  \"tagline\" : \"You Know, for Search\"\n"
                        + "}");
                return;
            }
            if ("/_search/scroll".equals(target)) {
                continueScroll(request, response);
                return;
            }
            target = trimTarget(target);
            final String[] parts = target.split("/");

            if ("_cluster".equals(parts[0])) {
                if ("health".equals(parts[1])) {
                    response.setContent("{\"cluster_name\":\"elasticsearch\",\"status\":\"green\",\"timed_out\":false,\"number_of_nodes\":1,\"number_of_data_nodes\":1,\"active_primary_shards\":1,\"active_shards\":1,\"relocating_shards\":0,\"initializing_shards\":0,\"unassigned_shards\":1,\"delayed_unassigned_shards\":0,\"number_of_pending_tasks\":0,\"number_of_in_flight_fetch\":0,\"task_max_waiting_in_queue_millis\":0,\"active_shards_percent_as_number\":50.0}");
                    return;
                }
            }

            final String indexName = parts[0];
            if (indexes == null || !indexes.containsKey(indexName)) {
                response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"},\"status\":404}");
                return;
            }

            final Index index = indexes.get(indexName);
            if (parts.length == 1) {
                response.setContent(gson.toJson(map(indexName, map(
                        "aliases", Collections.<String, Object>emptyMap(),
                        "mappings", index.mappings,
                        "settings", index.settings
                ))));
                return;
            }

            switch (parts[1]) {
                case "_count":
                    response.setContent("{\"count\":" + index.content.size() + ",\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0}}");
                    break;

                case "doc":
                case "_doc":
                    if (parts.length == 2) {
                        response.setContent("{\"error\":\"Incorrect HTTP method for uri [" + request.getTarget() + "] and method [GET], allowed: [POST]\",\"status\":405}");
                    }
                    else if (parts.length == 3 && "_search".equals(parts[2])) {
                        searchFromQueryParameters(queryParameters, index, parts[1], response);
                    }
                    else if (parts.length == 3) {
                        final String id = parts[2];
                        final JsonObject object = index.content.get(id);

                        final GetResponse<JsonObject> getResponse = new GetResponse<>(indexName, parts[1], id, 1, object != null, object);
                        response.setContent(gson.toJson(getResponse));
                    }
                    break;
                case "_mapping":
                    if (version.getMajorVersionNumber() >= 7) {
                        response.setContent(gson.toJson(map(
                            indexName, map(
                                "mappings", map(
                                    "properties", index.mappings)
                            ))));
                    } else {
                        response.setContent(gson.toJson(map(
                            indexName, map(
                                "mappings", map(
                                    "doc", map(
                                        "properties", index.mappings)
                                )))));
                    }
                    break;
                case "_search":
                    if (version.getMajorVersionNumber() >= 6) {
                        searchFromQueryParameters(queryParameters, index, "_doc", response);
                    }
                    break;
                case "_settings":
                    response.setContent(gson.toJson(map(
                            indexName, map(
                                "settings", map(
                                    "index", index.settings)
                            ))));
                    break;
            }
        } catch (IllegalArgumentException e) {
            sendError(400, e, response);
        } catch (Exception e) {
            sendError(500, e, response);
        }
    }

    @Override
    public void doPost(HttpRequest request, HttpResponse response) {
        response.setContentType(JSON_CONTENT_TYPE);
        response.setCharset(StandardCharsets.UTF_8);
        response.setContent("{\"error\":\"Incorrect HTTP method for uri [" + request.getTarget() + "] and method [POST]\",\"status\":405}");
        try {
            String target = request.getTarget();
            final HashMap<String, String> queryParameters = new HashMap<>();
            target = parseQueryParameters(target, queryParameters);
            switch (target) {
                case "/":
                    // Aucune action au chemin /
                    return;

                case "/_reindex":
                    if (!JSON_CONTENT_TYPE.equals(request.getContentType())) {
                        response.setContent("{\"error\":\"Content-Type header [" + request.getContentType() + "] is not supported\",\"status\":406}");
                        return;
                    }
                    final Map<String, Map<String, String>> parameters = gson.fromJson(request.getContent(), Map.class);
                    final String source = parameters.getOrDefault("source", Collections.emptyMap()).get("index");
                    final String destination = parameters.getOrDefault("dest", Collections.emptyMap()).get("index");
                    if (source == null || destination == null) {
                        response.setStatusCode(400);
                        response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"action_request_validation_exception\",\"reason\":\"Validation Failed: 1: index must be specified;\"}],\"type\":\"action_request_validation_exception\",\"reason\":\"Validation Failed: 1: index must be specified;\"},\"status\":400}");
                        return;
                    }
                    final Index sourceIndex = indexes.get(source);
                    if (sourceIndex == null) {
                        response.setStatusCode(404);
                        response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + source + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + source + "\",\"index_uuid\":\"_na_\",\"index\":\"" + source + "\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + source + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + source + "\",\"index_uuid\":\"_na_\",\"index\":\"" + source + "\"},\"status\":404}");
                        return;
                    }
                    Index destinationIndex = indexes.get(destination);
                    if (destinationIndex == null) {
                        destinationIndex = new Index();
                        destinationIndex.name = destination;
                        indexes.put(destination, destinationIndex);
                    }
                    destinationIndex.content.putAll(sourceIndex.content);
                    response.setContent("{\"took\":4686,\"timed_out\":false,\"total\":" + sourceIndex.content.size() + ",\"updated\":0,\"created\":" + sourceIndex.content.size() + ",\"deleted\":0,\"batches\":53,\"version_conflicts\":0,\"noops\":0,\"retries\":{\"bulk\":0,\"search\":0},\"throttled_millis\":0,\"requests_per_second\":-1.0,\"throttled_until_millis\":0,\"failures\":[]}");
                    return;

                case "/_bulk":
                    final String contentType = request.getHeader(HttpConstants.HEADER_CONTENT_TYPE);
                    if (!"application/json".equals(request.getContentType()) && !"application/x-ndjson".equals(contentType)) {
                        response.setStatusCode(406);
                        response.setContent("{\"error\":\"Content-Type header [" + contentType + "] is not supported\",\"status\":406}");
                        return;
                    }

                    final String content;
                    if (request.getCharset().equals(StandardCharsets.UTF_8)) {
                        content = request.getContent();
                    } else {
                        content = new String(request.getContent().getBytes(request.getCharset()), StandardCharsets.UTF_8);
                    }

                    final BulkResponse bulkResponse = new BulkResponse();
                    final String[] bulkJsons = content.split("\n");
                    for (int i = 0; i < bulkJsons.length; i += 2) {
                        final Map<String, Object> header = gson.fromJson(bulkJsons[i], Map.class);
                        final Map<String, Object> body = gson.fromJson(bulkJsons[i + 1], Map.class);

                        final Map.Entry<String, Object> entry = header.entrySet().iterator().next();
                        final Map<String, String> entryValue = ((Map<String, String>)entry.getValue());
                        final String operation = entry.getKey();
                        final String indexName = entryValue.get("_index");
                        final String documentType = entryValue.get("_type");
                        final String documentId = entryValue.getOrDefault("_id", generateIdentifier());

                        switch (operation) {
                            case "index":
                                add(indexName, documentId, body);
                                bulkResponse.addIndex(new CreateResponse(null, indexName, documentType, documentId, 1, 1, 0, null, Boolean.TRUE, null));
                                break;
                            default:
                                bulkResponse.getItems().add(new BulkResponseItem());
                                bulkResponse.setErrors(true);
                                break;
                        }
                    }
                    response.setContent(gson.toJson(bulkResponse));
                    return;

                case "/_msearch":
                    final ArrayList<SearchResponse<JsonObject>> searchResponses = new ArrayList<>();
                    final String[] searchJsons = request.getContent().split("\n");
                    for (int i = 0; i < searchJsons.length; i += 2) {
                        final String header = searchJsons[i];
                        final String body = searchJsons[i + 1];
                        final String indexName = (String)gson.fromJson(header, Map.class).get("index");

                        if (!indexExists(indexName)) {
                            searchResponses.add(gson.fromJson("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"},\"status\":404}", SearchResponse.class));
                        } else {
                            searchResponses.add(searchInIndex(indexes.get(indexName), "_doc", body, queryParameters));
                        }
                    }
                    response.setContent(gson.toJson(new MultiSearchResponse<>(searchResponses)));
                    return;

                case "/_search/scroll":
                    continueScroll(request, response);
                    return;

                default:
                    break;
            }

            if (!JSON_CONTENT_TYPE.equals(request.getContentType())) {
                response.setContent("{\"error\":\"Content-Type header [" + request.getContentType() + "] is not supported\",\"status\":406}");
                return;
            }

            target = trimTarget(target);
            final String[] parts = target.split("/");
            final String[] indexesName = parts[0].split(",");
            final Index[] indexArray = new Index[indexesName.length];
            for (int i = 0; i < indexesName.length; i++) {
                final String indexName = indexesName[i];
                if (!indexExists(indexName)) {
                    response.setStatusCode(404);
                    response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"},\"status\":404}");
                    return;
                }
                if (parts.length == 1) {
                    response.setStatusCode(405);
                    response.setContent("{\"error\":\"Incorrect HTTP method for uri [" + request.getTarget() + "] and method [POST], allowed: [PUT]\",\"status\":405}");
                    return;
                }
                indexArray[i] = indexes.get(indexName);
            }

            final Index index = indexArray[0];
            final String indexName = indexesName[0];
            switch (parts[1]) {
                case "_clone":
                    if (parts.length == 3) {
                        if (index.settings == null || index.settings.getOrDefault("index.blocks.write", Boolean.FALSE) != Boolean.TRUE) {
                            response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"index " + parts[0] + " must be read-only to resize index. use \\\"index.blocks.write=true\\\"\"}],\"type\":\"illegal_state_exception\",\"reason\":\"index " + parts[0] + " must be read-only to resize index. use \\\"index.blocks.write=true\\\"\"},\"status\":500}");
                            response.setStatusCode(500);
                            return;
                        }
                        Index clone = copyIndex(parts[2], index);
                        indexes.put(clone.name, clone);
                        response.setContent("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"" + clone.name + "\"}");
                    }
                    break;

                case "doc":
                case "_doc":
                    if (parts.length == 2) {
                        final JsonObject object = gson.fromJson(request.getContent(), JsonObject.class);
                        final String id = generateIdentifier();
                        add(indexName, id, object);
                        response.setContent(gson.toJson(new CreateResponse(new Shards(1, 1, 0, 0), indexName, parts[1], id, 1, 0, 1, "created", Boolean.TRUE, null)));
                    }
                    else if (parts.length == 3 && "_search".equals(parts[2])) {
                        final SearchResponse<JsonObject> searchResponse = searchInIndexes(indexArray, parts[1], request.getContent(), queryParameters);
                        response.setContent(gson.toJson(searchResponse));
                    }
                    else if (parts.length == 3) {
                        final JsonObject object = gson.fromJson(request.getContent(), JsonObject.class);
                        final String id = parts[2];
                        add(indexName, id, object);
                        response.setContent(gson.toJson(new CreateResponse(new Shards(1, 1, 0, 0), indexName, parts[1], id, 1, 0, 1, "created", Boolean.TRUE, null)));
                    }
                    break;

                case "_open":
                    if (parts.length == 2) {
                        response.setContent("{\"acknowledged\":true,\"shards_acknowledged\":true}");
                    }
                    break;

                case "_update_by_query":
                    final Map<String, Object> updateParameters = gson.fromJson(request.getContent(), Map.class);
                    final Map<String, Map<String, Object>> query = (Map<String, Map<String, Object>>) updateParameters.get("query");
                    final Map<String, Object> script = (Map<String, Object>) updateParameters.get("script");
                    if (!"painless".equals(script.get("lang"))) {
                        response.setStatusCode(400);
                        response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"illegal_argument_exception\",\"reason\":\"script_lang not supported [" + script.get("lang") + "]\"}],\"type\":\"illegal_argument_exception\",\"reason\":\"script_lang not supported [" + script.get("lang") + "]\"},\"status\":400}");
                        return;
                    }
                    final Painless painless = Painless.parse((String)script.get("source"), gson);
                    final Map<String, Object> scriptParameters = (Map<String, Object>)script.get("params");
                    for (final SearchResponseHit<JsonObject> document : index.search(parseQuery(query, index))) {
                        painless.execute(document, scriptParameters);
                        index.content.put(document.getId(), document.getSource());
                    }
                    response.setContent("{\"acknowledged\":true,\"shards_acknowledged\":true}");
                    break;
            }
        } catch (IllegalArgumentException e) {
            sendError(400, e, response);
        } catch (Exception e) {
            sendError(500, e, response);
        }
    }

    @Override
    public void doPut(HttpRequest request, HttpResponse response) {
        response.setContentType(JSON_CONTENT_TYPE);
        response.setCharset(StandardCharsets.UTF_8);

        if (!JSON_CONTENT_TYPE.equals(request.getContentType())) {
            response.setContent("{\"error\":\"Content-Type header [" + request.getContentType() + "] is not supported\",\"status\":406}");
            return;
        }

        response.setContent("{\"error\":\"Incorrect HTTP method for uri [" + request.getTarget() + "] and method [PUT]\",\"status\":405}");
        try {
            String target = request.getTarget();
            final HashMap<String, String> queryParameters = new HashMap<>();
            target = parseQueryParameters(target, queryParameters);
            if ("/".equals(target)) {
                response.setContent("{\"error\":\"Incorrect HTTP method for uri [/] and method [PUT], allowed: [GET]\",\"status\":405}");
                return;
            }
            target = trimTarget(target);
            final String[] parts = target.split("/");
            final String indexName = parts[0];
            if (parts.length == 1) {
                if (indexes == null) {
                    indexes = new HashMap<>();
                }
                final Index index = new Index();
                index.name = indexName;
                Map<String, Object> configuration = null;
                try {
                    configuration = gson.fromJson(request.getContent(), Map.class);
                } catch (JsonSyntaxException e) {
                    LOGGER.error("Bad JSON value: ", request.getContent(), e);
                }
                if (configuration != null) {
                    index.settings = (Map<String, Object>) configuration.get("settings");
                }
                indexes.put(indexName, index);
                response.setContent("{\"acknowledged\":true,\"shards_acknowledged\":true,\"index\":\"" + indexName + "\"}");
                return;
            }
            if (indexes == null || !indexes.containsKey(indexName)) {
                response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"},\"status\":404}");
                return;
            }

            final Index index = indexes.get(indexName);

            switch (parts[1]) {
                case "doc":
                case "_doc":
                    final String id = parts.length == 3 ? parts[2] : generateIdentifier();
                    index.content.put(id, gson.fromJson(request.getContent(), JsonObject.class));
                    response.setContent(gson.toJson(new CreateResponse(new Shards(1, 1, 0, 0), indexName, parts[1], id, 1, 0, 1, "created", Boolean.TRUE, null)));
                    break;
                case "_mapping":
                    final Map<String, Map<String, FieldMapping>> mapping = gson.fromJson(request.getContent(), new TypeToken<Map<String, Map<String, FieldMapping>>>() {}.getType());
                    index.setMappings(mapping.get("properties"));
                    response.setContent("{\"acknowledged\":true}");
                    break;
                case "_settings":
                    final Map<String, Map<String, Object>> settings = gson.fromJson(request.getContent(), new TypeToken<Map<String, Map<String, Object>>>() {}.getType());
                    index.settings = settings.get("settings");
                    response.setContent("{\"acknowledged\":true}");
                    break;
            }
        } catch (IllegalArgumentException e) {
            sendError(400, e, response);
        } catch (Exception e) {
            sendError(500, e, response);
        }
    }

    @Override
    public void doDelete(HttpRequest request, HttpResponse response) {
        response.setContentType(JSON_CONTENT_TYPE);
        response.setCharset(StandardCharsets.UTF_8);
        response.setContent("{\"error\":\"Incorrect HTTP method for uri [" + request.getTarget() + "] and method [DELETE]\",\"status\":405}");
        try {
            String target = request.getTarget();
            final HashMap<String, String> queryParameters = new HashMap<>();
            target = parseQueryParameters(target, queryParameters);
            if ("/".equals(target)) {
                response.setContent("{\"error\":\"Incorrect HTTP method for uri [/] and method [DELETE], allowed: [GET]\",\"status\":405}");
                return;
            }
            if ("/_search/scroll".equals(target)) {
                final Map<String, Object> body = gson.fromJson(request.getContent(), Map.class);
                final Object scrollId = body.get("scroll_id");
                final List<String> scrollIds;
                if (scrollId instanceof List) {
                    scrollIds = (List<String>)scrollId;
                } else if (scrollId instanceof String) {
                    scrollIds = Collections.singletonList((String)scrollId);
                } else {
                    response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"action_request_validation_exception\",\"reason\":\"Validation Failed: 1: scrollId is missing;\"}],\"type\":\"action_request_validation_exception\",\"reason\":\"Validation Failed: 1: scrollId is missing;\"},\"status\":400}");
                    return;
                }
                for (final String id : scrollIds) {
                    scrolls.remove(decodeScrollId(id));
                }
                response.setContent("{\"succeeded\":\"true\",\"num_freed\":" + scrollIds.size() + "}");
                return;
            }
            target = trimTarget(target);
            final String[] parts = target.split("/");
            final String indexName = parts[0];

            if (indexes == null || !indexes.containsKey(indexName)) {
                response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [" + indexName + "]\",\"resource.type\":\"index_or_alias\",\"resource.id\":\"" + indexName + "\",\"index_uuid\":\"_na_\",\"index\":\"" + indexName + "\"},\"status\":404}");
                return;
            }

            if (parts.length == 1) {
                indexes.remove(indexName);
                response.setContent("{\"acknowledged\":true}");
                return;
            }

            final Index index = indexes.get(indexName);

            switch (parts[1]) {
                case "doc":
                case "_doc":
                    if (parts.length == 3) {
                        final String documentId = parts[2];
                        final JsonObject removed = index.content.remove(documentId);
                        if (removed != null) {
                            response.setContent("{\"_index\":\"" + indexName + "\",\"_type\":\"" + parts[1] + "\",\"_id\":\"" + documentId + "\",\"_version\":2,\"result\":\"deleted\",\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0},\"_seq_no\":129,\"_primary_term\":34}");
                        } else {
                            response.setContent("{\"_index\":\"" + indexName + "\",\"_type\":\"" + parts[1] + "\",\"_id\":\"" + documentId + "\",\"_version\":3,\"result\":\"not_found\",\"_shards\":{\"total\":2,\"successful\":1,\"failed\":0},\"_seq_no\":130,\"_primary_term\":34}");
                        }
                    }
                    break;
            }

        } catch (IllegalArgumentException e) {
            sendError(400, e, response);
        } catch (Exception e) {
            sendError(500, e, response);
        }
    }

    /**
     * Récupère le numéro de version renvoyé par le mock.
     *
     * @return Numéro de version.
     */
    public String getVersion() {
        return version.toString();
    }

    /**
     * Modifie le numéro de version renvoyé par le mock.
     *
     * @param version Numéro de version.
     */
    public void setVersion(String version) {
        this.version = new VersionNumber(version);
    }

    public void createIndex(String indexName) {
        if (indexes == null) {
            indexes = new HashMap<>();
        }
        Index index = indexes.get(indexName);
        if (index == null) {
            index = new Index();
            index.name = indexName;
            indexes.put(indexName, index);
        }
    }

    public void add(String indexName, Object object) {
        add(indexName, object, gson);
    }

    public void add(String indexName, Object object, Gson gson) {
        add(indexName, generateIdentifier(), object, gson);
    }

    public void add(String indexName, String id, Object object) {
        add(indexName, id, object, gson);
    }

    public void add(String indexName, String id, Object object, Gson gson) {
        add(indexName, id, gson.toJsonTree(object).getAsJsonObject());
    }

    public void add(String indexName, String id, JsonObject object) {
        if (indexes == null) {
            indexes = new HashMap<>();
        }
        Index index = indexes.get(indexName);
        if (index == null) {
            index = new Index();
            index.name = indexName;
            indexes.put(indexName, index);
        }
        index.content.put(id, object);
    }

    public <T> T get(String indexName, String id, Class<T> clazz) {
        return get(indexName, id, clazz, gson);
    }

    public <T> T get(String indexName, String id, Class<T> clazz, Gson gson) {
        if (!indexExists(indexName)) {
            return null;
        }
        final Index index = indexes.get(indexName);
        return Optional.ofNullable(index.content.get(id))
                .map(json -> gson.fromJson(json, clazz))
                .orElse(null);
    }

    public <T> List<T> list(String indexName, Class<T> clazz) {
        return list(indexName, clazz, gson);
    }

    public <T> List<T> list(String indexName, Class<T> clazz, Gson gson) {
        if (indexes == null) {
            return Collections.emptyList();
        }
        final Index index = indexes.getOrDefault(indexName, new Index());
        return index.content.values().stream()
                .map(entry -> gson.fromJson(entry, clazz))
                .collect(Collectors.toList());
    }

    public boolean indexExists(String indexName) {
        return indexes != null && indexes.containsKey(indexName);
    }

    /**
     * Supprime le contenu des indexes et remet la version à 7.0.0.
     */
    public void clear() {
        version = new VersionNumber("7.0.0");
        indexes = null;
    }

    /**
     * Renvoi la liste des scrolls en cours d'itération.
     *
     * @return Scrolls actifs.
     */
    public Set<String> activeScrollIds() {
        return scrolls.keySet();
    }

    private Index copyIndex(final String name, final Index source) {
        final Index clone = new Index();
        clone.name = name;
        clone.content = new HashMap<>(source.content);
        if (source.mappings != null) {
            clone.mappings = new HashMap<>(source.mappings);
        }
        if (source.settings != null) {
            clone.settings = new HashMap<>(source.settings);
        }
        return clone;
    }

    private void searchFromQueryParameters(Map<String, String> queryParameters, Index index, String documentName, HttpResponse response) throws NumberFormatException {
        final List<SearchResponseHit<JsonObject>> queryResults;
        final int from = Integer.parseInt(queryParameters.getOrDefault("from", "0"));
        final int size = Integer.parseInt(queryParameters.getOrDefault("size", "10"));
        final String scrollDuration = queryParameters.get("scroll");
        final String queryString = queryParameters.get("q");
        if (queryString == null || queryString.isEmpty()) {
            queryResults = index.content.entrySet().stream()
                    .map(entry -> new SearchResponseHit<>(index.name, documentName, entry.getKey(), 1, entry.getValue()))
                    .collect(Collectors.toList());
        } else {
            response.setStatusCode(500);
            response.setContent("{\"error\":{\"reason\":\"Queries are not supported yet\"},\"status\":500}");
            return;
        }
        final String scrollId = startScroll(scrollDuration, queryResults, from, size);
        final int queryResultCount = queryResults.size();
        final List<SearchResponseHit<JsonObject>> results = queryResults.subList(Math.min(from, queryResultCount), Math.min(from + size, queryResultCount));
        final SearchResponse<JsonObject> searchResponse = new SearchResponse<>(42, false, scrollId, new Shards(1, 1, 0, 0), new SearchResponseHits<>(
                createTotal(queryResultCount, version),
                1,
                results
        ), null, null, null);
        response.setContent(gson.toJson(searchResponse));
    }

    private SearchResponse<JsonObject> searchInIndex(final Index index, final String docType, final String requestBody, final Map<String, String> queryParameters) {
        return searchInIndexes(new Index[] {index}, docType, requestBody, queryParameters);
    }

    private SearchResponse<JsonObject> searchInIndexes(final Index[] indexes, final String docType, final String requestBody, final Map<String, String> queryParameters) {
        final Map<String, Object> searchRequest = gson.fromJson(requestBody, Map.class);
        final ArrayList<SearchResponseHit<JsonObject>> queryResults = new ArrayList<>();

        final int from = ((Number) searchRequest.getOrDefault("from", 0)).intValue();
        final int size = ((Number) searchRequest.getOrDefault("size", 10)).intValue();
        final String scrollDuration = queryParameters.get("scroll");

        final Map<String, Map<String, Object>> query = (Map<String, Map<String, Object>>) searchRequest.get("query");
        for (final Index index : indexes) {
            Stream<SearchResponseHit<JsonObject>> stream;
            if (query == null || query.isEmpty()) {
                stream = index.content.entrySet().stream()
                        .map(entry -> new SearchResponseHit<>(index.name, docType, entry.getKey(), 1, entry.getValue()));
            } else {
                stream = index.searchStream(parseQuery(query, index));
            }
            stream.forEach(queryResults::add);
        }

        final Map<String, Object> aggregations = (Map<String, Object>)searchRequest.getOrDefault("aggregations", (Map<String, Object>)searchRequest.getOrDefault("aggs", Collections.emptyMap()));
        final HashMap<String, Object> aggregationResult = !aggregations.isEmpty() ? new HashMap<>() : null;
        for (Map.Entry<String, Object> aggregation : aggregations.entrySet()) {
            aggregationResult.put(
                    aggregation.getKey(),
                    parseAggregation((Map<String, Map<String, Object>>) aggregation.getValue())
                            .aggregate(queryResults));
        }

        // Tri des résultats.
        final List<Map<String, Map<String, String>>> sort = (List<Map<String, Map<String, String>>>) searchRequest.get("sort");
        if (sort != null) {
            queryResults.sort(parseSort(sort));
        } else {
            // Tri sur le score, du plus grand au plus petit.
            queryResults.sort((lhs, rhs) -> (int) (100.0 * (rhs.getScore() - lhs.getScore())));
        }

        // Scroll.
        final String scrollId = startScroll(scrollDuration, queryResults, from, size);

        // Pagination.
        final int queryResultCount = queryResults.size();
        final List<SearchResponseHit<JsonObject>> results = queryResults.subList(Math.min(from, queryResultCount), Math.min(from + size, queryResultCount));

        // Highlight
        final Map<String, Object> highlight = (Map<String, Object>) searchRequest.get("highlight");
        if (highlight != null && query != null) {
            final Map<String, Object> fieldsEntry = (Map<String, Object>) highlight.getOrDefault("fields", Collections.emptyMap());
            Set<String> fields = Optional.ofNullable(fieldsEntry)
                    .map(entry -> entry.keySet().stream()
                        .flatMap(field -> {
                            if (field.contains("*") || field.contains("?")) {
                                final Pattern pattern = Pattern.compile(globToPatternString(field));
                                return Stream.concat(
                                        indexes[0].mappings.keySet().stream()
                                                .filter(name -> pattern.matcher(name).matches()),
                                        indexes[0].copyToFields.keySet().stream()
                                                .filter(name -> pattern.matcher(name).matches()));
                            } else {
                                return Stream.of(field);
                            }
                        })
                        .collect(Collectors.toSet())
                    )
                    .orElse(Collections.emptySet());

            // TODO: Supporter le surlignage dans plusieurs index.
            final Highlighter highlighter = parseHighlighter(fields, query, indexes[0]);

            for (SearchResponseHit<JsonObject> hit : results) {
                final HashMap<String, List<String>> result = new HashMap<>();
                final JsonObject source = hit.getSource();
                mergeLists(result, highlighter.highlight(source));
                hit.setHighlight(result);
            }
        }

        // Filtrage de la source
        final Function<JsonObject, JsonObject> sourceMapper = parseSourceMapper(searchRequest.get("_source"));
        if (sourceMapper != null) {
            for (final SearchResponseHit<JsonObject> hit : results) {
                hit.setSource(sourceMapper.apply(hit.getSource()));
            }
        }

        return new SearchResponse<>(42, false, scrollId, new Shards(1, 1, 0, 0), new SearchResponseHits<>(
                createTotal(queryResultCount, version),
                1,
                results
        ), null, 200, aggregationResult);
    }

    private String startScroll(final String scrollDuration, final List<SearchResponseHit<JsonObject>> queryResults, final int from, final int size) {
        String scrollId = null;
        if (scrollDuration != null) {
            final String node = generateIdentifier();
            scrollId = encodeScrollId(node);
            final Scroll scroll = new Scroll();
            scroll.queryResults = queryResults;
            scroll.from = from + size;
            scroll.size = size;
            scroll.scrollId = scrollId;
            scrolls.put(node, scroll);
        }
        return scrollId;
    }

    private void continueScroll(HttpRequest request, HttpResponse response) {
        final Map<String, String> body = gson.fromJson(request.getContent(), Map.class);
        final String scrollId = body.get("scroll_id");
        if (scrollId == null) {
            response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"action_request_validation_exception\",\"reason\":\"Validation Failed: 1: scrollId is missing;\"}],\"type\":\"action_request_validation_exception\",\"reason\":\"Validation Failed: 1: scrollId is missing;\"},\"status\":400}");
            return;
        }
        final String node = decodeScrollId(scrollId);
        final Scroll scroll = scrolls.get(node);
        if (scroll == null) {
            response.setContent("{\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"node [" + node + "] is not available\"}],\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"query\",\"grouped\":true,\"failed_shards\":[{\"shard\":-1,\"index\":null,\"reason\":{\"type\":\"illegal_state_exception\",\"reason\":\"node [" + node + "] is not available\"}}]},\"status\":500}");
            return;
        }
        response.setContent(gson.toJson(scroll.nextPage(version)));
    }

    private String parseQueryParameters(String target, Map<String, String> queryParameters) throws UnsupportedEncodingException {
        final int queryStart = target.indexOf('?');
        if (queryStart < 0) {
            return target;
        }
        final String queryString = target.substring(queryStart + 1);
        for (String parameter : queryString.split("&")) {
            final String[] keyAndValue = parameter.split("=");
            queryParameters.put(keyAndValue[0], URLDecoder.decode(keyAndValue[1], "utf-8"));
        }
        return target.substring(0, queryStart);
    }

    private String trimTarget(String target) {
        if (target.charAt(0) == '/') {
            target = target.substring(1);
        }
        if (target.charAt(target.length() - 1) == '/') {
            target = target.substring(0, target.length() - 1);
        }
        return target;
    }

    private String generateIdentifier() {
        final StringBuilder builder = new StringBuilder();
        final char[] characters = new char[]{
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-'};
        for (int index = 0; index < 20; index++) {
            builder.append(characters[(int) (Math.random() * characters.length)]);
        }
        return builder.toString();
    }

    private String encodeScrollId(String identifier) {
        return Base64.getEncoder().encodeToString(("queryAndFetch\u0001\u0000\u0000\u0000\u0000\u0000\u0000\u0000>\u0016" + identifier).getBytes(StandardCharsets.US_ASCII));
    }

    private String decodeScrollId(String scrollId) {
        final String decodedId = new String(Base64.getDecoder().decode(scrollId), StandardCharsets.US_ASCII);
        return decodedId.substring(23);
    }

    /**
     * Renvoi une erreur en JSON contenant le message de l'exception donnée.
     *
     * @param statusCode Code d'état HTTP.
     * @param e Exception arrivée pendant le traitement.
     * @param response Réponse HTTP où écrire.
     */
    private void sendError(int statusCode, Exception e, HttpResponse response) {
        final HashMap<String, Object> content = new HashMap<>();
        final HashMap<String, Object> error = new HashMap<>();
        error.put("reason", e.getMessage());
        content.put("status", statusCode);
        content.put("error", error);
        response.setStatusCode(statusCode);
        response.setContent(gson.toJson(content));
    }

    private static SearchResponseHitsTotal createTotal(final long total, VersionNumber version) {
        if (version.getMajorVersionNumber() >= 7) {
            return new RelationSearchResponseHitsTotal(total, SearchResponseHitsTotalRelation.EQ);
        } else {
            return new LongSearchResponseHitsTotal(total);
        }
    }

    private static Query parseQuery(Map<String, Map<String, Object>> query, Index index) {
        final Map.Entry<String, Map<String, Object>> entry = query.entrySet().iterator().next();
        switch (entry.getKey()) {
            case "bool":
                return createBoolQuery(entry.getValue(), index);
            case "match":
                return createMatchQuery(entry.getValue(), index);
            case "wildcard":
                return createWildcardQuery(entry.getValue(), index);
            case "term":
                return createTermQuery(entry.getValue(), index);
            case "terms":
                return createTermsQuery(entry.getValue(), index);
            case "exists":
                return createExistsQuery(entry.getValue(), index);
            case "nested":
                return createNestedQuery(entry.getValue(), index);
            case "range":
                return createRangeQuery(entry.getValue(), index);
            default:
                LOGGER.warn("Given query is unsupported: ", entry.getKey());
                return NO_QUERY;
        }
    }

    private static Query createBoolQuery(final Map<String, Object> bool, final Index index) {
        final List<Map<String, Map<String, Object>>> must = (List<Map<String, Map<String, Object>>>) bool.get("must");
        final List<Map<String, Map<String, Object>>> mustNot = (List<Map<String, Map<String, Object>>>) bool.get("must_not");
        final List<Map<String, Map<String, Object>>> should = (List<Map<String, Map<String, Object>>>) bool.get("should");

        final List<Query> mustQueries = must != null ? must.stream().map(query -> parseQuery(query, index)).collect(Collectors.toList()) : Collections.emptyList();
        final List<Query> mustNotQueries = mustNot != null ? mustNot.stream().map(query -> parseQuery(query, index)).collect(Collectors.toList()) : Collections.emptyList();
        final List<Query> shouldQueries = should != null ? should.stream().map(query -> parseQuery(query, index)).collect(Collectors.toList()) : Collections.emptyList();

        return object -> {
            double score = shouldQueries.isEmpty() ? 1.0 : 0.0;
            for (final Query subQuery : mustQueries) {
                final double subScore = subQuery.rate(object);
                if (subScore <= 0.0) {
                    return 0.0;
                }
                score += subScore;
            }
            for (final Query subQuery : mustNotQueries) {
                final double subScore = subQuery.rate(object);
                if (subScore > 0.0) {
                    return 0.0;
                }
                score += subScore;
            }
            for (final Query subQuery : shouldQueries) {
                score += subQuery.rate(object);
            }
            return score;
        };
    }

    private static Query createMatchQuery(final Map<String, Object> match, final Index index) {
        final Map.Entry<String, Object> entry = match.entrySet().iterator().next();
        final String path = entry.getKey();
        final String queryString;
        if (entry.getValue() instanceof String) {
            queryString = (String) entry.getValue();
        } else if (entry.getValue() instanceof Map) {
            queryString = (String) ((Map<String, Object>) entry.getValue()).get("query");
        } else {
            queryString = null;
        }
        final String[] query = queryString != null
                ? queryString.toLowerCase().split(" ")
                : new String[0];
        return object -> {
            if (queryString == null || queryString.isEmpty()) {
                return 0.0;
            }

            final Collection<JsonPrimitive> primitives = getPrimitivesAtPath(path, object, index);

            double score = 0.0;
            for (final JsonPrimitive primitive : primitives) {
                final String primitiveValue;
                if (primitive.isString()) {
                    primitiveValue = primitive.getAsString().toLowerCase();
                } else if (primitive.isNumber()) {
                    primitiveValue = stringValueForNumber(primitive.getAsNumber());
                } else {
                    continue;
                }
                final String[] fieldValues = primitiveValue.split(" ");

                double term = 1.0;
                for (final String part : query) {
                    double distance = term;
                    for (final String fieldValue : fieldValues) {
                        if (fieldValue.equals(part)) {
                            score += distance;
                        }
                        distance = distance * 0.9;
                    }
                    term = term * 0.9;
                }
            }
            return score;
        };
    }

    private static Query createWildcardQuery(final Map<String, Object> wildcard, final Index index) {
        final Map.Entry<String, Object> entry = wildcard.entrySet().iterator().next();
        final String path = entry.getKey();
        final String queryString;
        if (entry.getValue() instanceof String) {
            queryString = (String) entry.getValue();
        } else if (entry.getValue() instanceof Map) {
            queryString = (String) ((Map<String, Object>) entry.getValue()).get("value");
        } else {
            queryString = "";
        }
        final Pattern pattern = Pattern.compile(globToPatternString(queryString));
        return object -> {
            final Collection<JsonPrimitive> primitives = getPrimitivesAtPath(path, object, index);

            double score = 0.0;
            for (final JsonPrimitive primitive : primitives) {
                if (!primitive.isString()) {
                    continue;
                }
                final String fieldValue = primitive.getAsString().toLowerCase();

                if (pattern.matcher(fieldValue).matches()) {
                    score += 1.0;
                }
            }
            return score;
        };
    }

    private static Query createTermQuery(final Map<String, Object> term, final Index index) {
        final Map.Entry<String, Object> entry = term.entrySet().iterator().next();
        final Object query;
        final double boost;
        if (entry.getValue() instanceof Map) {
            final Map<String, Object> value = (Map<String, Object>) entry.getValue();
            query = value.get("value");
            boost = ((Number)value.getOrDefault("boost", 1)).doubleValue();
        } else {
            query = entry.getValue();
            boost = 1.0;
        }
        final String queryAsString = stringValueFor(query);
        return object -> {
            if (query == null) {
                return 0.0;
            }

            final Collection<JsonPrimitive> primitives = getPrimitivesAtPath(entry.getKey(), object, index);

            double score = 0.0;
            for (final JsonPrimitive primitive : primitives) {
                final Object fieldValue = getPrimitiveValue(primitive);
                if (fieldValue.equals(query)) {
                    score += 1.0;
                } else if (stringValueFor(fieldValue).equals(queryAsString)) {
                    score += 0.8;
                }
            }
            return score * boost;
        };
    }

    private static Query createTermsQuery(final Map<String, Object> terms, final Index index) {
        final Object boostValue = terms.remove("boost");
        final double boost = boostValue instanceof Number ? ((Number)boostValue).doubleValue() : 1.0;

        final Map.Entry<String, Object> entry = terms.entrySet().iterator().next();
        final List<Object> queries = Optional.ofNullable((List<Object>) entry.getValue()).orElse(Collections.emptyList());
        final List<String> stringQueries = queries.stream()
                .map(Notela::stringValueFor)
                .collect(Collectors.toList());
        return object -> {
            if (queries.isEmpty()) {
                return 0.0;
            }

            final Collection<JsonPrimitive> primitives = getPrimitivesAtPath(entry.getKey(), object, index);

            double score = 0.0;
            for (final JsonPrimitive primitive : primitives) {
                final Object fieldValue = getPrimitiveValue(primitive);
                for (int i = 0; i < queries.size(); i++) {
                    final Object query = queries.get(i);
                    final String queryAsString = stringQueries.get(i);
                    if (fieldValue.equals(query)) {
                        score += 1.0;
                    } else if (stringValueFor(fieldValue).equals(queryAsString)) {
                        score += 0.8;
                    }
                }
            }
            return score * boost;
        };
    }

    private static Query createExistsQuery(final Map<String, Object> exists, final Index index) {
        final String path = (String)exists.get("field");
        if (path == null) {
            throw new IllegalArgumentException("[exists] requires 'field' field");
        }
        final FieldMapping mapping = index.mappings.get(path);
        if (mapping != null) {
            if (FieldMapping.TYPE_NESTED.equals(mapping.getType())) {
                return object -> 0.0;
            }
        } else {
            LOGGER.warn("Mapping for field '", path, "' of index '", index.name, "' has not been found, exists query may be incoherent with Elasticsearch.");
        }
        return object -> {
            final String fieldName = getLeafName(path);
            final Collection<JsonObject> nodes = getNodesBeforeLeaf(path, object);

            double score = 0.0;
            for (final JsonObject node : nodes) {
                if (node.has(fieldName)) {
                    score += 1.0;
                }
            }
            return score;
        };
    }

    private static Query createNestedQuery(final Map<String, Object> nested, final Index index) {
        final String path = (String) nested.get("path");
        if (path == null) {
            throw new IllegalArgumentException("[nested] requires 'path' field");
        }
        final Map<String, Map<String, Object>> query = (Map<String, Map<String, Object>>) nested.get("query");
        if (query == null) {
            throw new IllegalArgumentException("[nested] requires 'query' field");
        }
        final FieldMapping mapping = index.mappings.get(path);
        if (mapping != null) {
            if (!FieldMapping.TYPE_NESTED.equals(mapping.getType())) {
                throw new IllegalArgumentException("[nested] failed to find nested object under path [" + path + "]");
            }
        } else {
            LOGGER.warn("Mapping for path '", path, "' of index '", index.name, "' has not been found missing, nested query may be incoherent with Elasticsearch.");
        }
        final Index nestedIndex = new Index();
        nestedIndex.mappings = new HashMap<>(index.mappings);
        nestedIndex.mappings.put(path, FieldMapping.ofType("inside"));
        return parseQuery(query, nestedIndex);
    }

    private static Query createRangeQuery(final Map<String, Object> range, final Index index) {
        final Map.Entry<String, Object> entry = range.entrySet().iterator().next();
        final Map<String, Object> parameters = ((Map<String, Object>) entry.getValue());
        final Object greaterThanOrEquals = parameters.get("gte");
        final Object lessThanOrEquals = parameters.get("lte");

        return object -> {
            final Collection<JsonPrimitive> primitives = getPrimitivesAtPath(entry.getKey(), object, index);

            double score = 0.0;
            for (final JsonPrimitive primitive : primitives) {
                if (primitive.isNumber()) {
                    final Number value = primitive.getAsNumber();
                    int passed = 0;
                    int total = 0;
                    if (greaterThanOrEquals instanceof Number) {
                        total++;
                        passed += value.doubleValue() >= ((Number) greaterThanOrEquals).doubleValue() ? 1 : 0;
                    }
                    if (lessThanOrEquals instanceof Number) {
                        total++;
                        passed += value.doubleValue() <= ((Number) lessThanOrEquals).doubleValue() ? 1 : 0;
                    }
                    score += passed == total ? 1.0 : 0.0;
                }
                else if (primitive.isString()) {
                    LOGGER.warn("Range sur les dates pas encore supporté");
                }
            }
            return score;
        };
    }

    private static Comparator<SearchResponseHit<JsonObject>> parseSort(final List<Map<String, Map<String, String>>> sort) {
        return (lhs, rhs) -> {
            for (final Map<String, Map<String, String>> sortEntry : sort) {
                final Map.Entry<String, Map<String, String>> entry = sortEntry.entrySet().iterator().next();
                final String key = entry.getKey();
                final int order = "desc".equalsIgnoreCase(entry.getValue().get("order")) ? -1 : 1;

                final List<JsonPrimitive> leftValues = getPrimitivesAtPath(key, lhs.getSource(), null);
                final List<JsonPrimitive> rightValues = getPrimitivesAtPath(key, rhs.getSource(), null);

                final Object leftValue = leftValues.size() == 1 ? getPrimitiveValue(leftValues.get(0)) : null;
                final Object rightValue = rightValues.size() == 1 ? getPrimitiveValue(rightValues.get(0)) : null;
                if (leftValue == null) {
                    if (rightValue == null) {
                        continue;
                    } else {
                        return order;
                    }
                }
                if (rightValue == null) {
                    return -order;
                }
                if (leftValue.equals(rightValue)) {
                    continue;
                }
                if (leftValue instanceof Comparable) {
                    final int comparison = ((Comparable<Object>) leftValue).compareTo(rightValue);
                    if (comparison != 0) {
                        return comparison * order;
                    }
                }
            }
            return 0;
        };
    }

    private static Aggregation parseAggregation(Map<String, Map<String, Object>> aggregation) {
        final Map.Entry<String, Map<String, Object>> entry = aggregation.entrySet().iterator().next();
        switch (entry.getKey()) {
            case "terms":
                return createTermsAggregation(entry.getValue());
            case "composite":
                return createCompositeAggregation(entry.getValue());
            default:
                LOGGER.warn("Given aggregation is unsupported: ", entry.getKey());
                return hits -> Collections.emptyMap();
        }
    }

    private static Aggregation createTermsAggregation(Map<String, Object> terms) {
        final String field = (String)terms.get("field");
        final int size = ((Number)terms.getOrDefault("size", 10)).intValue();
        return hits -> {
            final HashMap<String, Integer> counts = new HashMap<>();
            for (final SearchResponseHit<JsonObject> hit : hits) {
                final Set<String> buckets;
                if ("_index".equals(field)) {
                    buckets = Collections.singleton(hit.getIndex());
                } else {
                    buckets = getPrimitivesAtPath(field, hit.getSource(), null).stream()
                            .filter(JsonPrimitive::isString)
                            .map(JsonPrimitive::getAsString)
                            .collect(Collectors.toSet());
                }
                for (final String bucket : buckets) {
                    counts.put(bucket, counts.getOrDefault(bucket, 0) + 1);
                }
            }
            final HashMap<String, Object> result = new HashMap<>();
            result.put("doc_count_error_upper_bound", 0);
            result.put("sum_other_doc_count", 0);
            result.put("buckets", counts.entrySet().stream()
                    .map(TermsBucket::new)
                    .sorted()
                    .limit(size)
                    .collect(Collectors.toList()));
            return result;
        };
    }

    private static Aggregation createCompositeAggregation(Map<String, Object> composite) {
        final int size = ((Number)composite.getOrDefault("size", 10)).intValue();
        final List<TermsCompositeAggregation> aggregations = TermsCompositeAggregation.from((List<Map<String, Map<String, Map<String, Object>>>>) composite.get("sources"));
        final Map<String, Object> after = (Map<String, Object>)composite.get("after");
        return hits -> {
            final HashMap<Map<String, Object>, Integer> counts = new HashMap<>();
            for (final SearchResponseHit<JsonObject> hit : hits) {
                List<Map<String, Object>> hitResults = Collections.emptyList();
                for (final TermsCompositeAggregation aggregation : aggregations) {
                    final Set<String> terms;
                    if ("_index".equals(aggregation.field)) {
                        terms = Collections.singleton(hit.getIndex());
                    } else {
                        terms = getPrimitivesAtPath(aggregation.field, hit.getSource(), null).stream()
                                .filter(JsonPrimitive::isString)
                                .map(JsonPrimitive::getAsString)
                                .collect(Collectors.toSet());
                    }
                    if (hitResults.isEmpty()) {
                        hitResults = terms.stream()
                                .map(term -> map(aggregation.name, (Object)term))
                                .collect(Collectors.toList());
                    } else {
                        hitResults = hitResults.stream()
                                .flatMap(hitResult -> terms.stream()
                                    .map(term -> {
                                        final HashMap<String, Object> termResults = new HashMap<>(hitResult);
                                        termResults.put(aggregation.name, term);
                                        return termResults;
                                    }))
                                .collect(Collectors.toList());
                    }
                }
                for (final Map<String, Object> hitResult : hitResults) {
                    final Integer count = counts.get(hitResult);
                    counts.put(hitResult, count != null ? count + 1 : 1);
                }
            }
            final boolean[] isAfter = new boolean[] { false };
            final int[] lastBucketIndex = new int[] { 0 };
            final List<Map<String, Object>> buckets = counts.entrySet().stream()
                    .sorted((lhs, rhs) -> {
                        for (final TermsCompositeAggregation aggregation : aggregations) {
                            final Object leftValue = lhs.getKey().get(aggregation.field);
                            final Object rightValue = rhs.getKey().get(aggregation.field);
                            final int order = ((Comparable<Object>)leftValue).compareTo(rightValue);
                            if (order != 0) {
                                return order * aggregation.order;
                            }
                        }
                        return 0;
                    })
                    .map(entry -> map("key", entry.getKey(), "doc_count", entry.getValue()))
                    .filter(entry -> {
                        lastBucketIndex[0]++;
                        if (after != null && !isAfter[0]) {
                            isAfter[0] = entry.equals(after);
                            return false;
                        }
                        return true;
                    })
                    .limit(size)
                    .collect(Collectors.toList());

            final HashMap<String, Object> resultMap = new HashMap<>();
            resultMap.put("buckets", buckets);
            if (lastBucketIndex[0] < counts.size() && !buckets.isEmpty()) {
                resultMap.put("after_key", buckets.get(buckets.size() - 1));
            }
            return resultMap;
        };
    }

    private static Function<JsonObject, JsonObject> parseSourceMapper(Object source) {
        if (source instanceof Boolean) {
            if (!(Boolean)source) {
                return hit -> null;
            }
        } else if (source instanceof List) {
            final List<String> fieldsToInclude = (List<String>)source;
            return hit -> {
                final JsonObject filtered  = new JsonObject();
                for (final String fieldToInclude : fieldsToInclude) {
                    final JsonElement value = hit.get(fieldToInclude);
                    if (value != null) {
                        filtered.add(fieldToInclude, value);
                    }
                }
                return filtered;
            };
        }
        return null;
    }

    /**
     * Récupère la valeur de l'objet JSON s'il s'agit d'une primitive.
     *
     * @param element Élément JSON.
     * @return Valeur en <code>Boolean</code>, <code>Number</code>,
     * <code>String</code> ou <code>null</code>.
     */
    private static Object getPrimitiveValue(JsonElement element) {
        if (element != null && element.isJsonPrimitive()) {
            final JsonPrimitive primitive = element.getAsJsonPrimitive();
            if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            }
            if (primitive.isNumber()) {
                // GSON converti tous les nombres en Double par défaut, donc
                // renvoi d'un double pour que les comparaisons fonctionnent.
                return primitive.getAsNumber().doubleValue();
            }
            if (primitive.isString()) {
                return primitive.getAsString();
            }
        }
        return null;
    }

    /**
     * Créé une nouvelle table en alternant la clef et la valeur.
     *
     * @param arguments Alternance de clefs et de valeurs.
     * @return Une nouvelle table avec le contenu donné.
     */
    private static <K, V> Map<K, V> map(final K key, final V value) {
        final HashMap<K, V> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    /**
     * Créé une nouvelle table en alternant la clef et la valeur.
     *
     * @param arguments Alternance de clefs et de valeurs.
     * @return Une nouvelle table avec le contenu donné.
     */
    private static Map<String, Object> map(final Object... arguments) {
        final HashMap<String, Object> map = new HashMap<>();
        for (int index = 0; index < arguments.length; index += 2) {
            map.put(arguments[index].toString(), arguments[index + 1]);
        }
        return map;
    }

    /**
     * Récupère les valeurs associées au chemin donné.
     *
     * @param path Chemin vers la donnée.
     * @param object Objet source.
     * @param index Si non null, permet de rechercher dans les champs "copy to".
     * @return Les valeurs primitives correspondantes.
     */
    private static List<JsonPrimitive> getPrimitivesAtPath(String path, JsonObject object, Index index) {
        if (path.endsWith(".keyword")) {
            path = path.substring(0, path.length() - ".keyword".length());
        }
        if (index != null && index.copyToFields.containsKey(path)) {
            return index.copyToFields.get(path).stream()
                    .map(object::get)
                    .filter(element -> element != null && element.isJsonPrimitive())
                    .map(JsonElement::getAsJsonPrimitive)
                    .collect(Collectors.toList());
        }
        return getElementsAtPath(path.split(Pattern.quote(".")), 0, object).stream()
                .filter(element -> element != null && element.isJsonPrimitive())
                .map(JsonElement::getAsJsonPrimitive)
                .collect(Collectors.toList());
    }

    private static Collection<JsonObject> getNodesBeforeLeaf(String path, JsonObject object) {
        final String[] parts = path.split(Pattern.quote("."));
        if (parts.length == 1) {
            return Collections.singleton(object);
        } else {
            final String[] partsExcludingLastEntry = new String[parts.length - 1];
            System.arraycopy(parts, 0, partsExcludingLastEntry, 0, partsExcludingLastEntry.length);
            return getElementsAtPath(partsExcludingLastEntry, 0, object).stream()
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .collect(Collectors.toList());
        }
    }

    private static Collection<JsonElement> getElementsAtPath(String[] parts, int index, JsonObject object) {
        JsonObject currentNode = object;
        for (; index < parts.length - 1; index++) {
            final JsonElement element = currentNode.get(parts[index]);
            if (element != null && element.isJsonObject()) {
                currentNode = element.getAsJsonObject();
            } else if (element != null && element.isJsonArray()) {
                final ArrayList<JsonElement> elements = new ArrayList<>();
                final JsonArray array = element.getAsJsonArray();
                for (int arrayIndex = 0; arrayIndex < array.size(); arrayIndex++) {
                    final JsonElement arrayEntry = array.get(arrayIndex);
                    if (arrayEntry != null && arrayEntry.isJsonObject()) {
                        elements.addAll(getElementsAtPath(parts, index + 1, arrayEntry.getAsJsonObject()));
                    }
                }
                return elements;
            } else {
                return Collections.emptySet();
            }
        }
        return Collections.singleton(currentNode.get(parts[parts.length - 1]));
    }

    private static String getLeafName(String path) {
        final int dotIndex = path.lastIndexOf('.');
        return dotIndex < 0 ? path : path.substring(dotIndex + 1);
    }

    private static String stringValueFor(Object value) {
        if (value instanceof Number) {
            return stringValueForNumber((Number)value);
        } else {
            return value != null ? value.toString() : "";
        }
    }

    private static String stringValueForNumber(Number number) {
        final double value = number.doubleValue();
        if (Math.floor(value) == Math.ceil(value)) {
            return Long.toString((long)value);
        } else {
            return Double.toString((long)value);
        }
    }

    private static String globToPatternString(final String queryString) {
        return queryString
                .replace(".", "\\.")
                .replace("[", "\\[")
                .replace("(", "\\(")
                .replace("?", ".")
                .replace("*", ".*");
    }

    private static void mergeLists(final HashMap<String, List<String>> currentResults, final Map<String, List<String>> newResults) {
        for (Map.Entry<String, List<String>> entry : newResults.entrySet()) {
            final List<String> currentValue = currentResults.get(entry.getKey());
            if (currentValue != null) {
                currentValue.addAll(entry.getValue());
            } else {
                currentResults.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private static class Index {
        String name;
        Map<String, FieldMapping> mappings = new HashMap<>();
        Map<String, Object> settings = new HashMap<>();
        Map<String, JsonObject> content = new LinkedHashMap<>();
        Map<String, List<String>> copyToFields = new HashMap<>();

        List<SearchResponseHit<JsonObject>> search(Query query) {
            return searchStream(query)
                    .collect(Collectors.toList());
        }

        Stream<SearchResponseHit<JsonObject>> searchStream(Query query) {
            return content.entrySet().stream()
                    .map(entry -> new SearchResponseHit<>(name, "_doc", entry.getKey(), query.rate(entry.getValue()), entry.getValue()))
                    .filter(hit -> hit.getScore() > 0.0);
        }

        public void setMappings(Map<String, FieldMapping> mappings) {
            this.mappings = mappings;

            final HashMap<String, List<String>> map = new HashMap<>();
            for (Map.Entry<String, FieldMapping> mapping : mappings.entrySet()) {
                for (final String field : mapping.getValue().copyToFields()) {
                    List<String> sources = map.get(field);
                    if (sources == null) {
                        sources = new ArrayList<>();
                        map.put(field, sources);
                    }
                    sources.add(mapping.getKey());
                }
            }
            this.copyToFields = map;
        }
    }

    private static class Scroll {
        List<SearchResponseHit<JsonObject>> queryResults;
        int from;
        int size;
        String scrollId;

        SearchResponse<JsonObject> nextPage(final VersionNumber version) {
            final int queryResultCount = queryResults.size();
            final List<SearchResponseHit<JsonObject>> results = queryResults.subList(Math.min(from, queryResultCount), Math.min(from + size, queryResultCount));
            from += size;

            return new SearchResponse<>(42, false, scrollId, new Shards(1, 1, 0, 0), new SearchResponseHits<>(
                    createTotal(queryResultCount, version),
                    1,
                    results
            ), null, null, null);
        }
    }

    private static interface Query {
        double rate(JsonObject object);
    }

    private static interface Aggregation {
        Map<String, Object> aggregate(List<SearchResponseHit<JsonObject>> hits);
    }

    private static class TermsCompositeAggregation {
        final String name;
        final String field;
        final int order;

        public TermsCompositeAggregation(String name, String field, int order) {
            this.name = name;
            this.field = field;
            this.order = order;
        }

        static List<TermsCompositeAggregation> from(List<Map<String, Map<String, Map<String, Object>>>> sources) {
            final ArrayList<TermsCompositeAggregation> aggregations = new ArrayList<>();
            for (final Map<String, Map<String, Map<String, Object>>> source : sources) {
                final Map.Entry<String, Map<String, Map<String, Object>>> entry = source.entrySet().iterator().next();
                final String name = entry.getKey();

                final Map.Entry<String, Map<String, Object>> sourceAggregation = entry.getValue().entrySet().iterator().next();

                switch (sourceAggregation.getKey()) {
                case "terms":
                    final String field = (String)sourceAggregation.getValue().get("field");
                    final int order = "desc".equals(sourceAggregation.getValue().getOrDefault("order", "asc")) ? -1: 1;
                    aggregations.add(new TermsCompositeAggregation(name, field, order));
                    break;

                case "histogram":
                case "date_histogram":
                default:
                    LOGGER.warn("Given composite aggregation is not supported: " + sourceAggregation.getKey());
                    break;
                }
            }
            return aggregations;
        }
    }

    private static interface Highlighter {
        Map<String, List<String>> highlight(JsonObject object);
    }

    private static Highlighter parseHighlighter(Set<String> fieldsToHighlight, Map<String, Map<String, Object>> query, Index index) {
        final Map.Entry<String, Map<String, Object>> entry = query.entrySet().iterator().next();
        switch (entry.getKey()) {
            case "bool":
                return createBoolHighlighter(fieldsToHighlight, entry.getValue(), index);
            case "match":
                return createMatchHighlighter(fieldsToHighlight, entry.getValue(), index);
            case "wildcard":
                return createWildcardHighlighter(fieldsToHighlight, entry.getValue(), index);
            case "term":
                return createTermHighlighter(fieldsToHighlight, entry.getValue(), index);
            case "terms":
                return createTermsHighlighter(fieldsToHighlight, entry.getValue(), index);
            default:
                return NO_HIGHLIGHTER;
        }
    }

    private static Highlighter createBoolHighlighter(Set<String> fieldsToHighlight, Map<String, Object> bool, Index index) {
        // TODO: Supporter ça
        return NO_HIGHLIGHTER;
    }

    private static Highlighter createMatchHighlighter(Set<String> fieldsToHighlight, Map<String, Object> match, Index index) {
        final Map.Entry<String, Object> entry = match.entrySet().iterator().next();
        final String queryString;
        if (entry.getValue() instanceof String) {
            queryString = (String) entry.getValue();
        } else if (entry.getValue() instanceof Map) {
            queryString = (String) ((Map<String, Object>) entry.getValue()).get("query");
        } else {
            queryString = null;
        }
        final String key = entry.getKey();
        if (queryString == null || !fieldsToHighlight.contains(key)) {
            return NO_HIGHLIGHTER;
        }
        final Pattern pattern = Pattern.compile(
                Arrays.stream(queryString.toLowerCase().split(" "))
                    .map(Pattern::quote)
                    .collect(Collectors.joining("|")), Pattern.CASE_INSENSITIVE);
        return PatternHighlighter.forField(key, index, pattern);
    }

    private static Highlighter createWildcardHighlighter(Set<String> fieldsToHighlight, Map<String, Object> wildcard, Index index) {
        final Map.Entry<String, Object> entry = wildcard.entrySet().iterator().next();
        final String queryString;
        if (entry.getValue() instanceof String) {
            queryString = (String) entry.getValue();
        } else if (entry.getValue() instanceof Map) {
            queryString = (String) ((Map<String, Object>) entry.getValue()).get("query");
        } else {
            queryString = null;
        }
        final String key = entry.getKey();
        if (queryString == null || !fieldsToHighlight.contains(key)) {
            return NO_HIGHLIGHTER;
        }
        final Pattern pattern = Pattern.compile(globToPatternString(queryString), Pattern.CASE_INSENSITIVE);
        return PatternHighlighter.forField(key, index, pattern);
    }

    private static Highlighter createTermHighlighter(Set<String> fieldsToHighlight, Map<String, Object> term, Index index) {
        final Map.Entry<String, Object> entry = term.entrySet().iterator().next();
        final Object query;
        if (entry.getValue() instanceof Map) {
            final Map<String, Object> value = (Map<String, Object>) entry.getValue();
            query = value.get("value");
        } else {
            query = entry.getValue();
        }
        final String key = entry.getKey();
        if (query == null || !fieldsToHighlight.contains(key)) {
            return NO_HIGHLIGHTER;
        }
        final Pattern pattern = Pattern.compile(Pattern.quote(stringValueFor(query)));
        return PatternHighlighter.forField(key, index, pattern);
    }

    private static Highlighter createTermsHighlighter(Set<String> fieldsToHighlight, Map<String, Object> terms, Index index) {
        // TODO: Supporter ça
        return NO_HIGHLIGHTER;
    }

    private static class CompositeHighlighter implements Highlighter {
        public static Highlighter withHighlighters(Collection<Highlighter> highlighters) {
            final List<Highlighter> filteredHighlighters = highlighters.stream()
                    .filter(highlighter -> highlighter != NO_HIGHLIGHTER)
                    .collect(Collectors.toList());
            if (filteredHighlighters.isEmpty()) {
                return NO_HIGHLIGHTER;
            } else {
                return new CompositeHighlighter(filteredHighlighters);
            }
        }

        private final Collection<Highlighter> highlighters;

        public CompositeHighlighter(Collection<Highlighter> highlighters) {
            this.highlighters = highlighters;
        }

        @Override
        public Map<String, List<String>> highlight(JsonObject object) {
            final HashMap<String, List<String>> result = new HashMap<>();
            for (final Highlighter highlighter : highlighters) {
                mergeLists(result, highlighter.highlight(object));
            }
            return result;
        }
    }

    private static class PatternHighlighter implements Highlighter {
        public static Highlighter forField(String key, Index index, Pattern pattern) {
            if (index.copyToFields.containsKey(key)) {
                return PatternHighlighter.forFieldsWithSamePattern(index.copyToFields.get(key), pattern);
            } else {
                return new PatternHighlighter(key, pattern);
            }
        }

        public static Highlighter forFieldsWithSamePattern(Collection<String> keys, Pattern pattern) {
            if (keys.isEmpty()) {
                return NO_HIGHLIGHTER;
            } else if (keys.size() == 1) {
                return new PatternHighlighter(keys.iterator().next(), pattern);
            } else {
                return CompositeHighlighter.withHighlighters(keys.stream()
                        .map(key -> new PatternHighlighter(key, pattern))
                        .collect(Collectors.toList()));
            }
        } 

        private final String key;
        private final Pattern pattern;

        public PatternHighlighter(String key, Pattern pattern) {
            this.key = key;
            this.pattern = pattern;
        }

        @Override
        public Map<String, List<String>> highlight(JsonObject object) {
            final JsonElement element = object.get(key);
            if (element != null && element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
                final String value = element.getAsString();
                final Matcher matcher = pattern.matcher(value);
                if (matcher.find()) {
                    final ArrayList<String> values = new ArrayList<>();
                    values.add(pattern.matcher(value).replaceAll("<em>$0</em>"));
                    return map(key, values);
                }
            }
            return Collections.emptyMap();
        }
    }

}

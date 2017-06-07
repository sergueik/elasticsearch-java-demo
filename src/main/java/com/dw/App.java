package com.dw;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.UUID;

import org.boon.json.JsonCreator;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import spark.ModelAndView;
import spark.Spark;
import spark.template.mustache.MustacheTemplateEngine;

/**
 * Elastic Search Demo App
 * 
 * @author Matt Tyson
 */
public class App {
	protected static ObjectMapper mapper = JsonFactory.create();

	public static void main(String[] args) throws Exception {
		Client client = TransportClient.builder().build().addTransportAddress(
				new InetSocketTransportAddress(InetAddress.getByName("localhost"),
						9300));

		Spark.get("/", (request, response) -> {
			SearchResponse searchResponse = client.prepareSearch("music")
					.setTypes("lyrics").execute().actionGet();
			SearchHit[] hits = searchResponse.getHits().getHits();

			Map<String, Object> attributes = new HashMap<>();
			attributes.put("songs", hits);

			return new ModelAndView(attributes, "index.mustache");
		}, new MustacheTemplateEngine());
		Spark.get("/search", (request, response) -> {
			SearchRequestBuilder srb = client.prepareSearch("music")
					.setTypes("lyrics");

			String lyricParam = request.queryParams("query");
			QueryBuilder lyricQuery = null;
			if (lyricParam != null && lyricParam.trim().length() > 0) {
				lyricQuery = QueryBuilders.matchQuery("lyrics", lyricParam);
				// srb.setQuery(qb).addHighlightedField("lyrics", 0, 0);
			}
			String artistParam = request.queryParams("artist");
			QueryBuilder artistQuery = null;
			if (artistParam != null && artistParam.trim().length() > 0) {
				artistQuery = QueryBuilders.matchQuery("artist", artistParam);
			}

			if (lyricQuery != null && artistQuery == null) {
				( srb.setQuery(lyricQuery)).addHighlightedField("lyrics", 0, 0);
			} else if (lyricQuery == null && artistQuery != null) {
				srb.setQuery(artistQuery);
			} else if (lyricQuery != null && artistQuery != null) {
				srb.setQuery(QueryBuilders.andQuery(artistQuery, lyricQuery))
						.addHighlightedField("lyrics", 0, 0);
			}

			SearchResponse searchResponse = srb.execute().actionGet();

			SearchHit[] hits = searchResponse.getHits().getHits();

			Map<String, Object> attributes = new HashMap<>();
			attributes.put("songs", hits);

			return new ModelAndView(attributes, "index.mustache");
		}, new MustacheTemplateEngine());
		Spark.get("/add", (request, response) -> {
			return new ModelAndView(new HashMap(), "add.mustache");
		}, new MustacheTemplateEngine());
		Spark.post("/save", (request, response) -> {
			StringBuilder json = new StringBuilder("{");
			json.append("\"name\":\"" + request.raw().getParameter("name") + "\",");
			json.append(
					"\"artist\":\"" + request.raw().getParameter("artist") + "\",");
			json.append("\"year\":" + request.raw().getParameter("year") + ",");
			json.append("\"album\":\"" + request.raw().getParameter("album") + "\",");
			json.append(
					"\"lyrics\":\"" + request.raw().getParameter("lyrics") + "\"}");

			IndexRequest indexRequest = new IndexRequest("music", "lyrics",
					UUID.randomUUID().toString());
			indexRequest.source(json.toString());
			IndexResponse esResponse = client.index(indexRequest).actionGet();

			Map<String, Object> attributes = new HashMap<>();
			return new ModelAndView(attributes, "index.mustache");
		}, new MustacheTemplateEngine());
	}
}

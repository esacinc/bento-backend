package gov.nih.nci.bento.controller;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import gov.nih.nci.bento.error.ApiError;
import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.model.GraphQlQuery;
import gov.nih.nci.bento.service.Neo4JGraphQLService;
import gov.nih.nci.bento.service.RedisService;
import graphql.language.Document;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.Null;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@RestController
public class GraphQLController {

	private static final Logger logger = LogManager.getLogger(GraphQLController.class);

	@Autowired
	private ConfigurationDAO config;
	@Autowired
	private Neo4JGraphQLService neo4jService;
	@Autowired
	private RedisService redisService;
	
	public static final Gson GSON = new Gson();

	@CrossOrigin
	@RequestMapping(value = "/version", method = {RequestMethod.GET})
	public ResponseEntity<String> getVersion(HttpEntity<String> httpEntity, HttpServletResponse response){
		logger.info("Hit end point:/version");
		String versionString = "Bento API Version: "+config.getBentoApiVersion();
		logger.info(versionString);
		return ResponseEntity.ok(versionString);
	}

	@CrossOrigin
	@RequestMapping
			(value = "/v1/graphql/", method = {
				RequestMethod.GET, RequestMethod.HEAD, RequestMethod.PUT, RequestMethod.DELETE,
				RequestMethod.TRACE, RequestMethod.OPTIONS, RequestMethod.PATCH}
			)
	public ResponseEntity<String> getGraphQLResponseByGET(HttpEntity<String> httpEntity, HttpServletResponse response){
		HttpStatus status = HttpStatus.METHOD_NOT_ALLOWED;
		String error = ApiError.jsonApiError(new ApiError(status, "API will only accept POST requests"));
		return logAndReturnError(status, error);
	}

	@CrossOrigin
	@RequestMapping(value = "/set-testing", method = RequestMethod.POST)
	@ResponseBody
	public ResponseEntity<String> getGraphQLSetResponse(HttpEntity<String> httpEntity, HttpServletResponse response) {
		logger.info("hit end point:/set-testing");
		String reqBody = httpEntity.getBody();
		ArrayList<String> keys = new ArrayList<>();
		for(String category: redisService.getGroups()){
			try{
				keys.add(storeUnion(category, reqBody));
			}
			catch (NullPointerException e){}
		}
		String responseText;
		String subjectIds = String.join("\",\"", redisService.getIntersection(keys.toArray(new String[0])));
//		subjectIds = "\""+subjectIds+"\"";
//		try{
//			String req = String.format("{\"query\":\"query (\\r\\n    $subject_ids: [String],\\r\\n    $first: Int\\r\\n)\\r\\n{\\r\\n    searchSubjectsWithGroupLists(\\r\\n        subject_ids:$subject_ids,\\r\\n        first:$first\\r\\n    )\\r\\n    {\\r\\n        numberOfPrograms\\r\\n        numberOfStudies\\r\\n        numberOfSamples\\r\\n        numberOfLabProcedures\\r\\n        numberOfFiles\\r\\n        subjectIds\\r\\n        firstPage{\\r\\n            subject_id\\r\\n            program\\r\\n            program_id\\r\\n            study_acronym\\r\\n            study_short_description\\r\\n            study_info\\r\\n            diagnosis\\r\\n            recurrence_score\\r\\n            tumor_size\\r\\n            tumor_grade\\r\\n            er_status\\r\\n            pr_status\\r\\n            chemotherapy\\r\\n            endocrine_therapy\\r\\n            menopause_status\\r\\n            age_at_index\\r\\n            survival_time\\r\\n            survival_time_unit\\r\\n            samples\\r\\n            lab_procedures\\r\\n        }\\r\\n    }\\r\\n}\",\"variables\":{\"subject_ids\":[%s],\"first\":10000}}",subjectIds);
//			responseText = neo4jService.query(req);
//		}
//		catch(ApiError e){
//			String error = ApiError.jsonApiError(e);
//			return logAndReturnError(e.getStatus(), error);
//		}
		return ResponseEntity.ok(subjectIds);
	}

	private String storeUnion(String category, String reqBody) throws NullPointerException{
		JsonObject jsonObject = GSON.fromJson(reqBody, JsonObject.class);
		List<String> list = GSON.fromJson(jsonObject.get(category), List.class);
		String[] values = list.toArray(new String[0]);
		for(int i = 0; i < values.length; i++){
			values[i] = category+":"+values[i];
		}
		String key = category+"Union";
		redisService.unionStore(key, values);
		return key;
	}

	@CrossOrigin
	@RequestMapping(value = "/v1/graphql/", method = RequestMethod.POST)
	@ResponseBody
	public ResponseEntity<String> getGraphQLResponse(HttpEntity<String> httpEntity, HttpServletResponse response){

		logger.info("hit end point:/v1/graphql/");

		// Get graphql query from request
		String reqBody = httpEntity.getBody().toString();
		Gson gson = new Gson();
		JsonObject jsonObject = gson.fromJson(reqBody, JsonObject.class);
		String operation;
		String queryKey;
		try{
			String sdl = new String(jsonObject.get("query").getAsString().getBytes(), "UTF-8");
			Parser parser = new Parser();
			Document document = parser.parseDocument(sdl);
			queryKey = document.toString();
			JsonElement rawVar = jsonObject.get("variables");
			if (null != rawVar) {
				queryKey +=  "::" + rawVar.toString();
			}
			OperationDefinition def = (OperationDefinition) document.getDefinitions().get(0);
			operation = def.getOperation().toString().toLowerCase();
		}
		catch(Exception e){
			HttpStatus status = HttpStatus.BAD_REQUEST;
			String error = ApiError.jsonApiError(status, "Invalid query in request", e.getMessage());
			return logAndReturnError(status, error);
		}

		if ((operation.equals("query") && config.isAllowGraphQLQuery())
				|| (operation.equals("mutation") && config.isAllowGraphQLMutation())) {
			try{
				String responseText;
				if (config.getRedisEnabled()) {
					responseText = redisService.getCachedValue(queryKey);
					if ( null == responseText) {
						responseText = neo4jService.query(reqBody);
						redisService.cacheValue(queryKey, responseText);

						/*
						String searchQueryName = "searchSubjects(";
						String newReqBody;
						while(reqBody.contains(" (")){
							reqBody = reqBody.replace(" (", "(");
						}
						if(config.getRedisSetsEnabled() && reqBody.contains(searchQueryName)){
							newReqBody = convertToGroupListQuery(reqBody, searchQueryName);
							responseText = neo4jService.query(reqBody);
							if (null == responseText){
								responseText = neo4jService.query(reqBody);
								redisService.cacheValue(queryKey, responseText);
							}
							else{
								redisService.cacheValue(newReqBody, responseText);
							}
						}*/
					}
				} else {
					responseText = neo4jService.query(reqBody);
				}
				return ResponseEntity.ok(responseText);
			}
			catch(ApiError e){
				String error = ApiError.jsonApiError(e);
				return logAndReturnError(e.getStatus(), error);
			}
		}
		else if(operation.equals("query") || operation.equals("mutation")){
			HttpStatus status = HttpStatus.FORBIDDEN;
			String error = ApiError.jsonApiError(status, "Request type has been disabled", operation+" operations have been disabled in the application configuration.");
			return logAndReturnError(status, error);
		}
		else {
			HttpStatus status = HttpStatus.BAD_REQUEST;
			String error = ApiError.jsonApiError(status, "Unknown operation in request", operation+" operation is not recognized.");
			return logAndReturnError(status, error);
		}

	}

	// Split into queries 
	// Seperate into requests
	// Execute both queries
	// Recombine
	@CrossOrigin
	@RequestMapping(
		value = "/demo/graphql/", 
		method = RequestMethod.POST
	)
	@ResponseBody
	public ResponseEntity<String> sortQueries(HttpEntity<String> httpEntity, HttpServletResponse response)
	throws ApiError {
		logger.info("hit end point: /demo/graphql/");
										
		Yaml yaml = new Yaml();
		List<String> queries = new ArrayList<String>();
										
										
		try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(config.getRedisSetsQueries())) {
			Map<String, List<String>> obj = yaml.load(inputStream);
			queries = obj.get("queries");
			// System.out.println("queries: " + queries);
		} catch(IOException | YAMLException e) {
										
		}
										
		// Get graphql query from request
		String reqBody = httpEntity.getBody().toString();
		// System.out.println("reqbody : " + reqBody);
		Gson gson = new Gson();
		JsonObject jsonObject = gson.fromJson(reqBody, JsonObject.class);
		String variables = jsonObject.get("variables").toString();
		String operation;
		String queryKey;
		String responseText = "";
		// System.out.println("jsonObject----> " + jsonObject);
										
		try {
			String sdl = new String(jsonObject.get("query").getAsString().getBytes(), "UTF-8");
			StringTokenizer st = new StringTokenizer(sdl);
			// System.out.println("Next Token is : " + st.nextToken());
			String token;
			String requestOne = "";
			String requestTwo = "";
			String head = st.nextToken("\n");
			requestOne += head;
			requestTwo += head;
			while(st.hasMoreTokens()){
				token = st.nextToken("}");
				if(stringContains(token,queries)){
					token += "}";
					requestOne += token;
				}else {
					token += "}";
					requestTwo += token;
				}
			}
			requestOne += "}";
										
			GraphQlQuery query = new GraphQlQuery(requestOne,variables);
										
			Gson gsonBuilder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.IDENTITY).setPrettyPrinting().create();
			String testRequest = gsonBuilder.toJson(query);
			// System.out.println("testRequest:"+ testRequest);
										
													
			// System.out.println("requestOne: " + requestOne + "requestOne ends");
			// System.out.println("requestTwo: " + requestTwo + "requestTwo ends");
													
													
			responseText = neo4jService.query(testRequest);
										
			// Parser parser = new Parser();
			// Document document = parser.parseDocument(requestOne);
			// queryKey = document.toString();
			// JsonElement rawVar = jsonObject.get("variables");
			// System.out.println("document ------>" + document);
		} catch(Exception e) {
										
		}									
		return ResponseEntity.ok(responseText);
	}

	private String convertToGroupListQuery(String original, String queryName){
		String[] splitOnQueryName = original.split(queryName);
		String before = splitOnQueryName[0];
		int braceIndex = splitOnQueryName[1].indexOf('{');
		String after = splitOnQueryName[1].substring(braceIndex);
		String queryCall = splitOnQueryName[1].substring(0, braceIndex);
		ArrayList<String> filters = new ArrayList<>();
		int openIndex = queryCall.indexOf('[');
		while(openIndex != -1){
			int closeIndex = queryCall.indexOf(']');
			filters.add(queryCall.substring(openIndex+1, closeIndex));
			queryCall = queryCall.substring(closeIndex+1);
			openIndex = queryCall.indexOf('[');
		}
		String paramsString = String.join(",", filters);
		return before + "searchSubjectsWithGroupLists(subject_ids:[" + paramsString + "])" + after;
	}

	private ResponseEntity logAndReturnError(HttpStatus status, String error){
		logger.error(error);
		return ResponseEntity.status(status).body(error);
	}

	public static boolean stringContains(String input, List<String> queries) {
		return queries.stream().anyMatch(input::contains);
	   // return queries.stream().filter(input::contains).findAny().map(Object::toString).orElse("");
   }
}

package gov.nih.nci.bento;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nih.nci.bento.controller.GraphQLController;
import gov.nih.nci.bento.error.ApiError;
import gov.nih.nci.bento.model.ConfigurationDAO;
import gov.nih.nci.bento.service.Neo4JGraphQLService;
import gov.nih.nci.bento.service.RedisService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Component
public class ApplicationInitializer implements InitializingBean {

    private static final Logger logger = LogManager.getLogger(ApplicationInitializer.class);

    @Autowired
    private ConfigurationDAO config;
    @Autowired
    private Neo4JGraphQLService neo4jService;
    @Autowired
    private RedisService redisService;

    public static final Gson GSON = new Gson();


    @Override
    public void afterPropertiesSet() throws Exception {
        if(redisService.isInitialized() && config.getRedisSetsEnabled()){
            initGroupListsInCache();
        }
    }

    private void initGroupListsInCache(){
        logger.info("Initializing group list in Redis");
        Yaml yaml = new Yaml();
        try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(config.getRedisSetsQueries())) {
            Map<String, List<String>> obj = yaml.load(inputStream);
            List<String> queries = obj.get("queries");
            for(String query: queries){
                try{
                    String response = neo4jService.query(String.format("{\"query\":\"{%s{group, subjects}}\"}", query));
                    JsonObject jsonObject = GSON.fromJson(response, JsonObject.class);
                    jsonObject = jsonObject.getAsJsonObject("data");
                    JsonArray jsonArray = jsonObject.getAsJsonArray(query);
                    for(JsonElement element: jsonArray){
                        JsonObject groupList = element.getAsJsonObject();
                        String group = removeQuotes(groupList.get("group").toString());
                        String[] subjects = formatSubjects(groupList.get("subjects").toString());
                        logger.info("Caching "+group);
                        redisService.cacheGroup(group, subjects);
                    }
                } catch (ApiError apiError) {
                    logger.error(apiError.getStatus());
                    logger.error(apiError.getErrors());
                    logger.error(apiError.getMessage());
                    logger.warn("Error occurred while querying Neo4j to initialize group lists");
                }
                catch (Exception e){
                    logger.error(e.getMessage());
                    logger.warn("Exception occurred while initializing group lists");
                }
            }
        } catch (IOException | YAMLException e) {
            logger.error(e);
            logger.warn("Unable to read set queries from file");
        }
    }

    private String removeQuotes(String input){
        return input.replaceAll("\"", "");
    }

    private String[] formatSubjects(String subjectsString){
        subjectsString = removeQuotes(subjectsString);
        subjectsString = subjectsString.replace("[", "");
        subjectsString = subjectsString.replace("]", "");
        return subjectsString.split(",");
    }

}

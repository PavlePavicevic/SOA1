package com.weather.gateway;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

@RestController
@RequestMapping("/api/weather")
public class WeatherSummaryController {

  private final WebClient webClient;

  @Value("${app.datasetServiceUrl}")
  private String datasetServiceUrl; 

  @Value("${app.weatherstackKey}")
  private String weatherstackKey; 

  public WeatherSummaryController(WebClient.Builder builder) {
    this.webClient = builder.build(); 
  }

  @GetMapping("/summary")
  public Map<String, Object> summary(
      @RequestParam String city,
      @RequestParam(required = false) String from,
      @RequestParam(required = false) String to
  ) {
    
    String datasetUrl = datasetServiceUrl + "/observations?city=" + city
        + (from != null ? "&from=" + from : "")
        + (to != null ? "&to=" + to : ""); 

    List<Map<String, Object>> history = webClient.get()
        .uri(datasetUrl)
        .retrieve()
        .bodyToMono(List.class)
        .block();

    Map<String, Object> historyStats = computeStats(history);
    
    String wsUrl = "https://api.weatherstack.com/current?access_key=" + weatherstackKey + "&query=" + city;

    Map<String, Object> weatherstack = webClient.get()
        .uri(wsUrl)
        .retrieve() 
        .bodyToMono(Map.class)
        .block();

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("city", city);
    result.put("current", weatherstack);
    result.put("history", historyStats);
    result.put("data_sources", List.of("weatherstack", "mongo-dataset-service"));
    return result;
  } 


  private Map<String, Object> computeStats(List<Map<String, Object>> history) {
    if (history == null || history.isEmpty()) {
      return Map.of("count", 0);
    }


    int count = 0;
    double tempSum = 0;
    double precipSum = 0;


    for (Map<String, Object> row : history) {
      count++;

      Object t = row.get("temperature");
      if (t instanceof Number n) tempSum += n.doubleValue();

      Object p = row.get("precipitation");
      if (p instanceof Number n) precipSum += n.doubleValue();
    }
    double tempAvg = count > 0 ? tempSum / count : 0;
  
    Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("count", count);
    stats.put("temperature_avg", tempAvg);
    stats.put("precipitation_sum", precipSum);
    return stats;
  }
}

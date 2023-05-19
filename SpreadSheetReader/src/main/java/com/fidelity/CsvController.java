package com.fidelity;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.Reader;
import java.io.InputStreamReader;
import java.util.*;

@RestController
@RequestMapping("/api/csv")
public class CsvController {

    @PostMapping
    public ResponseEntity<Map<String, List<Map<String, String>>>> uploadCSVFile(@RequestParam("file") MultipartFile file) {
        if (!file.isEmpty()) {
            try {
                Map<String, List<Map<String, String>>> result = parseCSV(file);

                // Initialize RestTemplate
                RestTemplate restTemplate = new RestTemplate();

                // Loop over the result map
                for (Map.Entry<String, List<Map<String, String>>> entry : result.entrySet()) {

                    // Create process variable in the format required by Camunda API
                    Map<String, Object> valueMap = new HashMap<>();
                    valueMap.put("value", entry.getValue());

                    Map<String, Object> camundaVariables = new HashMap<>();
                    camundaVariables.put("processData", valueMap);

                    // URL of the Camunda API
                    String url = "http://localhost:8080/engine-rest/process-definition/key/TestProcess/start";

                    // Prepare the payload for the Camunda API
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("variables", camundaVariables);

                    // Create a new HttpEntity object with the payload as the body
                    HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload);

                    // Execute the HTTP POST request
                    ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

                    // Check the HTTP response status
                    if (!response.getStatusCode().equals(HttpStatus.OK)) {
                        throw new RuntimeException("Failed to start process instance: " + response.getBody());
                    }
                }
                return new ResponseEntity<>(result, HttpStatus.OK);
            } catch (Exception e) {
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }
        } else {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
    }

    private Map<String, List<Map<String, String>>> parseCSV(MultipartFile file) {
        Map<String, List<Map<String, String>>> csvData = new HashMap<>();

        try (Reader reader = new InputStreamReader(file.getInputStream())) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            for (CSVRecord record : records) {
                Map<String, String> row = new HashMap<>();
                String key = "";
                for (String column : record.toMap().keySet()) {
                    if (column.equals("paymentMethod")) {  // Replace "Number" with your column name
                        key = record.get(column);
                        row.put(column, key);
                    } else {
                        row.put(column, record.get(column));
                    }
                }
                csvData.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse CSV file: " + e.getMessage());
        }

        return csvData;
    }
}

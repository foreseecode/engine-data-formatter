package com.foresee.dataformatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.foresee.platform.engine.core.Datapoint;
import com.foresee.platform.engine.core.EngineOptions;
import com.foresee.platform.engine.core.EngineRequestMetadata;
import com.foresee.platform.engine.core.InputDataItem;
import com.foresee.platform.engine.core.api.EngineRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Map;

@Service
public class DataFormatter {

    private static final Integer REQUEST_TYPE_SCORES = 1;
    private static final Integer REQUEST_TYPE_IMPACTS = 2;
    private static final Integer REQUEST_TYPE_NPS = 3;

    @Value("${app.request-type}")
    private Integer requestType;

    @Value("${app.client-id}")
    private Long clientId;

    @Value("${app.measurement-id}")
    private Long measurementId;

    @Value("${app.data-file}")
    private String dataFile;

    @Value("${app.output-file}")
    private String outputFile;

    public void execute() throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        DatabaseObjectList surveyData = objectMapper.readValue(
                new File(dataFile),
                DatabaseObjectList.class);

        EngineRequest engineRequest = new EngineRequest();
        EngineRequestMetadata.RequestTypeEnum requestTypeEnum = EngineRequestMetadata.RequestTypeEnum.SCORES;
        if (REQUEST_TYPE_IMPACTS.equals(requestType)) {
            requestTypeEnum = EngineRequestMetadata.RequestTypeEnum.IMPACTS;
        }
        engineRequest.setRequestMetadata(EngineRequestMetadata.builder()
                .clientKey(clientId.toString())
                .key(measurementId.toString())
                .requestType(requestTypeEnum)
                .options(new EngineOptions())
                .build());

        Map<String, InputDataItem> requestDataMap = Maps.newLinkedHashMap();
        InputDataItem.DatatypeEnum datatypeEnum = InputDataItem.DatatypeEnum.QUESTION_ANSWER;
        if (REQUEST_TYPE_IMPACTS.equals(requestType)) {
            datatypeEnum = InputDataItem.DatatypeEnum.LATENT_SCORE;
        }
        for (DatabaseObject item : surveyData.getItems()) {
            InputDataItem inputDataItem = requestDataMap.get(item.getId());
            if (inputDataItem == null) {
                inputDataItem = InputDataItem.builder()
                        .key(item.getRespondentKey())
                        .id(item.getId())
                        .timestamp(item.getTimestamp())
                        .datapoints(Lists.newArrayList())
                        .build();
                requestDataMap.put(item.getId(), inputDataItem);
            }
            inputDataItem.setDatatype(datatypeEnum);
            inputDataItem.getDatapoints().add(new Datapoint(item.getKey(), item.getValue()));
        }
        engineRequest.setRequestData(Lists.newArrayList(requestDataMap.values()));
        objectMapper.writeValue(new File(outputFile), engineRequest);
    }
}

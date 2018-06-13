package com.foresee.dataformatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.foresee.platform.engine.core.*;
import com.foresee.platform.engine.core.api.EngineRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.List;
import java.util.Map;

@Service
public class DataFormatter {

    private static final Integer REQUEST_TYPE_SCORES = 1;
    private static final Integer REQUEST_TYPE_IMPACTS = 2;

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
        EngineRequestMetadata.RequestTypeEnum requestTypeEnum = EngineRequestMetadata.RequestTypeEnum.SCORES_AND_IMPACTS;
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

    public void filterScores() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        EngineResponse engineResponse = objectMapper.readValue(
                new File("calculatedScores.json"),
                EngineResponse.class);

        List<String> listFilter = Lists.newArrayList("IVQZ94U08M9UxE5xgskZAg4C","0pYNYUwNEIRl4wsIY5oxgA4C","xAJVk04VhE499YZwgl8RgA4C","QldswAclx4V5BF9kQcp8dg4C","RR1p1RdocdNhhEUNEF9B8w4C","08Uxw4MMNkE0Etox8No0cg4C","5JQUN1lIlEYRhF9p1hw9Vw4C","IN4RV5c4kIoRtVosNkV90w4C","1tIQ5N4F4sJMY48AlQhMgg4C","4g8Uclkwl1h1x1E8JgggNw4C","0wwpkIZcFwAJ0oZgwlMoNQ4C","5llY1Nx5I59s4w49NUFNNQ4C","IQpQ4FQJdtxkVRRJ4Rt5gA4C","gVMZc0ZpUEsVZN8FwQN1tQ4C","MA8MttkYdwEQ4d1Y1ppl8w4C","Adg90h5BYJJ5lREwIsdEZw4C","4UssQhs5sBdARF5Uo5oAlg4C","xB9NZ4pkQ8MklJYI8sBYQQ4C","NJ1tdlUAUBcFBQUkgpdJ4g4C","U4cMR05ElEk9Jk1I49VcVA4C","UVcoUgwNxNM8cYxBwV1N4w4C","Vg4B1V9ctx8otcRZN9NE0Q4C","89cs9YU8cMoY0NwwZJscsA4C","l1opJg00EQBIZR8VhgM5AQ4C","w1hMpxohBcsp1FNEI50Yww4C","4oQtQUUk5Ak19VgRcxs1YA4C","Z9x00g8skM5sREI4RpNIpA4C","5Fhx4lsBFIcZd4Jwk1xQAg4C","hko1NZ0x4oRoAAFAls0dAA4C","NtI89Mtoc4w8Qk1dogMBog4C","s0JdcIUdItAxhIFssB0Alg4C","0pQQBI94NF8EMEFhRkA9tQ4C","940oRllJRU4xcBV9p58x8g4C","FkB0A481NxlYsJsk5QIhEg4C","NpdBgUpQo4R5UsAs8c9Zgg4C","EU4RR4o9k9lQNot5oBs0AA4C","w41Qs8Qoo0s40Nwh1FpoAA4C","YJwgVAxAF9sRh0sAAoF1Ig4C","BhFZhoBJVMd0IABcBo98Bg4C","AUF5oUkws5wB4lpI5Y84UA4C","Z1QYQoA1cZhZx4RlAgxI4g4C","ZU0JZQFxUNsdIFN0Zpcwtw4C","g8Yp4pZlJhc1cAB0JVhsIA4C","AkR00Fo1QlFh9IF0UcZ4Fw4C","RdxpVBsVV49IJ8RR145hxA4C","ooY8QhEVA0MENR8AkEcV4g4C","99tsYkFVwQsNZw01MtNZBg4C","tYxh9toxQ001VNAxwVwk5g4C","EMIYwlZ1xUIdc4wYNZJp8A4C","INotYAB9E8QM1oJMFNJQkw4C","R5V1J5Ax8JEQQRR9NMlpNg4C","5BUpdYVQsAhw89E4wFl5FQ4C","5gI0cB5gMxN15l8JxoB18A4C","V0pl8AhItdg5AI5915QdRQ4C","pRxUBk9IFYBpRosApgdVdQ4C","9oosZsEJNdd9d1EAZVc4Ug4C","xtIpMt95M044pk00ABEtlQ4C","ANdY0dRUF5YBhhk8xJ0MJg4C","8QVFFVcBktsRwx1JB9JAQA4C","xR9x8h4gxpMZ1cNQlNEdAg4C","1EVFAdVE4JxYpsEUBp1xMQ4C","8xpQVlZkYQ91dk15cwg90g4C","FE0tQsMJ5811xVIh4lNkMQ4C","FJxsxEJA0shUoEFBUI4ARA4C","pRRAQM495BVF4wQhlU0AAg4C","hMw4tp9AJhhIpVQMoJ1xkQ4C","IAAB8V1hc4MhE554sptAVg4C","FhY9EhpMFkQA1slB9pwx8w4C","Fsw8Mt1145YItE91ABBsMg4C","xd9EVEkI4hssdlkVAA0kEA4C","YVVtVBV5QJ5Uh0ZoAN8xoQ4C","BFthMplstVApYotAAYZlFw4C","hJ4Mw5IMIwNUtQNoIJARkA4C","AkcJ40xQBQlkQFRwIJd1Yw4C","g90RJ44tgMFkA9QMZMEVkg4C","YlBBJRdcRco4tsBVo1N0tw4C","1A4BZwcdkUhA1s4AdFAgIA4C","BgUAFYY45hQp5xMpMoVMNA4C","RVEVB4QE9Q9xh15sE9wZUA4C","JxlYtds1Zhsgh1ZRMoslVg4C","BtNZg98kgU9FJ55IU54Fww4C","h0MIUxlw4NQ5VVYE4R5I5g4C","BcIJ48stZYpJ9hsNspIllw4C","lYgsU9QZsBQB1tZpl1cRBw4C","NFksRsZUoxtwAspoIFFloA4C","MsEsk0NsEtMxF484xwRQIw4C","gEAcc0hdt5FgMlpN9V8Ycw4C","JhIo4MBNFcQgd5BU5txZFQ4C","Rs4gIYtIg545o0osYwQxsA4C","x98xJhVVpIdAJ0wlYUBVBg4C","RZdJ00k01EZFgFkYVYgxdg4C","og81cR4IIIVc9JQtsIBZcQ4C","hMYJdcs0pV8BhQhAMFtRgQ4C","sEx9dVB4RF4h9t0NcMhIFA4C","Jcgd4poABZocNlN0UIlMxg4C","shp1M95NgZgAVIY1h8QU1Q4C","swxoVMhgEYRhllIMkJ5xZQ4C","NMxARU9QZ08htNwUsZZlhg4C","goYdgApY9BNcMp99Bpxh1w4C","Vkkl4s0ctx8hY4ZNw40hxw4C","YNYwo9gRM8sVpF8ksJFYUA4C","JJpAIABpBJIFQwQMd5ZY1g4C","R4BhVUthdE4BxttUN5F1Qw4C","cts5N0VEV0UQUV5QNhIMkg4C","1dZcwEU1kUQwohMwsVgs0Q4C","VcchgxNA0JEVwUhdwAogtQ4C","kVUMA9dYI0gkwRAt0ZhNsQ4C","9hp45F0g1NtQcBcoZpIAQA4C","Y4toVFZYARYd5tgI1AlkJw4C","stJVV0IY9c5VMQ1UZYZklQ4C","ZcYR8dxYN0Yg9Z504lBkUw4C","1IhtMgQEsYQg05AQthpQMw4C","kx4AI1cY14R1Ntok8ZcJ0A4C","4Ew8EhNdpIxt8Jgo9YpA9g4C","8FUdp8YUxQJFtJh8A1hF0A4C","YIpIccsNVgkZIU4hl4ctBQ4C","QE8ZdZg8lpIhEwpQo1UcJA4C","4l4MAM5BQUJtgVkdRBUdtw4C","9h8s4ABQ4td0g4909xtZlw4C","40k80ctkAZ5U09AIkZpRtw4C","8lA9xV8JYkIxsQRZY5shEw4C","0ENYsJd9xxVUh0sB91Z0dg4C","oYwV5JgNY9tFt0k1Uhk5Ig4C","tRYsw1pptlEF9YwYpY8p9g4C","FI9UMZAh1kwAZZwQ0VxVUw4C","sJMhpEAwoN0Q1FBVdU9gwg4C","1AgAMs5FUppkN5dZMZxt0Q4C","sctE0tZpE4gtQ4EksR01Aw4C","EdwY5Etk04hZs0VdFF9EAg4C","xlUQNV45oAY50tYJM00kNw4C","8RAN00ZQYlc0ARUBtBA9ow4C","ZFN8hVIRRA0N4MQ1tEUJRQ4C","hYQhdNUFAdIh8QpQQFRFNA4C","95hR0VVoU1Mg5N1F5MVk9Q4C","8F9gJZ0ZwcgApwkNl85QZQ4C","58RUAhB4tNQoMYgUwMkEoA4C","9V9lkwd055xoMJUlc1p1Nw4C","ZBhR0Q9kBpRVkkJlM9UosA4C","RVB1dUJFQ58sNI0dAl91Fg4C","pg8hxgdsxkkNNxNUgkBEVg4C","d9Ik5lQl0F5MVgpJRZBkJQ4C","FlkxQthcJNkFUwR4Agg1kw4C","cV0wRcc1cx8lp4hMVhx9sQ4C","Fstg9MRERpJcN8stxYIYpw4C","csR0tx8YVksMk8tMo9RpoQ4C","tw9FVZxFokRsBEdNdp8cVw4C","JJ5Mgoh19E0Z49EsN0gkFA4C","kdEJMMN8IcREZ9FwU1pQcw4C","tUBotpoodYYY4BRVEZ9t8Q4C","l0ZsNp4gN5xI0lZpsJ58Bg4C","dBEk5pEBMVgZQAE5tUgB9g4C","0Isc8ksB9tdsMkMtg4E4dQ4C","J5BEkVBgMltt98oAF8AYpA4C","gB0s4AI1F5JIJgN5k9hAxA4C","REJwBAUgEUEwo1pgUkVc0g4C","hJQQ4xEkcFQQtht4Y5ZBlw4C","Fk4NghFw9I9VtZwENMMVVg4C","EpphMFoZx9cRUVUxcEk8kQ4C","UxAdg0Fk9At8wBs8FI1gkg4C","hQJYolt4EFkI851xkkpZIQ4C","t94lgV8h0opJ9dMM55h1MQ4C","VVUI0gFkBIthRMMlcpUwZQ4C","kB4QJJhUl05kh9N8oF0MlA4C","1sdQQYc8IN88kl8UsJBIdg4C","ko40JM9NMxsA4pUV9tJIsQ4C","4ZcBY8VE1AccYMcQ9p9Ugg4C","AR88tM0hwBksVA4Mc9oxZw4C","Upw5w8ZRBsFsVMZI1xxJhQ4C","1J4x8AQIAZIo8g4JYBYZpw4C","csxRpY5lUY5U5AkMtQQZkA4C","YRMNU9l4wolVEFAEd4k8NA4C","14twMMsoMdh8EsAUJVw4pg4C","1dxYJkYllN8R4JMxB4cwkQ4C","xRU80Ux9hctdlREsEBQ9cg4C","YxJQZQp8xdpUlgsNkwQsZw4C","V15EphVwJJAJFd908B0spg4C","dRgA1tcU00E8NxQ454EMog4C","VcdYRRNwsQwMFVY5RI4hVA4C","RxQZFZgJMNM81RtQE8Aolg4C","c8dgJhRUBIsRkkI0lQZQdg4C","oV18gFQBIIQwZh0F80BgpQ4C");
        List<LatentScores> filteredScores = Lists.newArrayList();
        for (LatentScores item : engineResponse.getLatentScores()) {
            if (listFilter.contains(item.getMeasuredVariableId())) {
                filteredScores.add(item);
            }
        }
        engineResponse.setLatentScores(filteredScores);
        objectMapper.writeValue(new File("calculatedScoresFiltered.json"), engineResponse);
    }
}

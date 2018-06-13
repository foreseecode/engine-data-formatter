package com.foresee.dataformatter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

/**
 * @author dustin.benac
 * @since 2/13/2017
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DatabaseObject {
    private Long respondentKey;
    private String id;
    private ZonedDateTime timestamp;
    private String key;
    private BigDecimal value;
}

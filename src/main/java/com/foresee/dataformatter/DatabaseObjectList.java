package com.foresee.dataformatter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author dustin.benac
 * @since 2/13/2017
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DatabaseObjectList {

    List<DatabaseObject> items;
}

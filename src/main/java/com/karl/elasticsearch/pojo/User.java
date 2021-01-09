package com.karl.elasticsearch.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * @author karl xie
 * Created on 2021-01-09 10:27
 */
@Data
@Builder
@Component
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private String name;

    private int age;

}
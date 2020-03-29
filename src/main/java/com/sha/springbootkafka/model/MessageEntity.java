package com.sha.springbootkafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @author sa
 * @date 2020-03-28
 * @time 10:58
 */
@NoArgsConstructor // public MessageEntity();
@AllArgsConstructor // public MessageEntity(name, title)
@Data //This will provide getter, setter...
public class MessageEntity
{
    private String name;
    private LocalDateTime time;
}

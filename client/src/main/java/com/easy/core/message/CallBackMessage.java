package com.easy.core.message;

import com.easy.core.entity.MessageId;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
public class CallBackMessage  implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * 要回应的messageId
     */
    public MessageId answerTo;
    /**
     * 真正要传递的数据
     */
    public byte[] data;

    /**
     * 回调对象的类型
     */
    Class<?> callBackDataClass;





}

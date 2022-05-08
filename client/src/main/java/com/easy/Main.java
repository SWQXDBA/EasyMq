package com.easy;

import com.easy.core.entity.MessageId;

import java.util.HashMap;

/**
 * @author SWQXDBA
 */
public class Main {

    public static void main(String[] args) {
        EasyClient client = new EasyClient(8080,"localhost");
        client.addListener("", new EasyListener(HashMap.class) {
            @Override
            public void handle(MessageId messageId, Object message) {
                System.out.println("收到了"+message);
                if(message instanceof HashMap){
                    HashMap<String,String> map = (HashMap)message;
                    System.out.println("data: "+map.get("data"));
                }
            }
        });

        client.run();
    }
}

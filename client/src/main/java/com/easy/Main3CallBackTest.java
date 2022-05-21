package com.easy;

import com.easy.core.entity.MessageId;
import com.easy.core.listener.CallBackListener;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main3CallBackTest {
    public static void main(String[] args) {



        EasyClient client = new EasyClient(8080, "localhost", "group2", "消费者9");
        client.addListener(new CallBackListener<String, Person>("topic") {
            @Override
            public Person answer(MessageId messageId, String message) {
                client.confirmationResponse(messageId);
                final Person person = new Person();

                return person;
            }
        });


        final ExecutorService service = Executors.newFixedThreadPool(100);
        service.execute(() -> {
                Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                final String s = scanner.nextLine();

                for (int i = 0; i < 100; i++) {

                    final long time1 = LocalDateTime.now().getLong(ChronoField.MILLI_OF_DAY);
                    final Future<Person> personFuture = client.sendAsync(s, "topic");
                    final Person person;
                    try {
                        person = personFuture.get();
                        System.out.println("应答时间"+(person.localDateTime.getLong(ChronoField.MILLI_OF_DAY)-time1));
                        System.out.println("回调时间:"+(LocalDateTime.now().getLong(ChronoField.MILLI_OF_DAY)-time1));
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }




//                    final long time1 = LocalDateTime.now().getLong(ChronoField.MILLI_OF_DAY);
//                    final Person person = client.sendSync(s, "topic");
//                    System.out.println("应答时间"+(person.localDateTime.getLong(ChronoField.MILLI_OF_DAY)-time1));
//                    System.out.println("回调时间:"+(LocalDateTime.now().getLong(ChronoField.MILLI_OF_DAY)-time1));



//                    final long time1 = LocalDateTime.now().getLong(ChronoField.MILLI_OF_DAY);
//                    //  System.out.println("发送时间"+time1);
//
//                    client.sendAsync(s, "topic", (Consumer<Person>) person -> {
//                        System.out.println("应答时间"+(person.localDateTime.getLong(ChronoField.MILLI_OF_DAY)-time1));
//                        System.out.println("回调时间:"+(LocalDateTime.now().getLong(ChronoField.MILLI_OF_DAY)-time1));
//                    });
                }
            }
        });


        client.run();
    }
}

package br.com.sqs_consomer.services;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.LocalDate;
import java.util.List;

public class SQSService {
    public static void messageReader(){
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider() {
            @Override
            public AwsCredentials resolveCredentials() {
                return new AwsCredentials() {
                    @Override
                    public String accessKeyId() {
                        return System.getenv("AWS_ACCESS_KEY");
                    }
        
                    @Override
                    public String secretAccessKey() {
                        return System.getenv("AWS_SECRET_KEY");
                    }
                };
            }
        };

        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(credentialsProvider)
                .build();

        
        String awsId = System.getenv("AWS_ACCOUNT_ID");       
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()               
                .queueName("queue_poc_ecommerce")  
                .queueOwnerAWSAccountId(awsId).build();
        GetQueueUrlResponse createResult = sqsClient.getQueueUrl(request);
        
        List<Message> messages = receiveMessages(sqsClient, createResult.queueUrl());
      
        for (Message mess : messages) {
            
            String toMe = "pedido1";
            String from = mess.body();

            System.out.println("Mensagem: " + from);
            if ( from.equals(toMe)){
                System.out.println("ENVIANDO PARA APROVACAO DE CREDITO");
                deleteMessages(sqsClient, createResult.queueUrl(),  messages);
                sendMessage(sqsClient, createResult.queueUrl(), "transacao1");
            } else if (from.equals("transacao1")) {
                System.out.println("APROVACAO , enviando NotaFiscal");
                deleteMessages(sqsClient, createResult.queueUrl(),  messages);
                sendMessage(sqsClient, createResult.queueUrl(), "notaFiscal");                
            } else if (from.equals("notaFiscal")){
                System.out.println("APROVACAO , enviando despacho");
                deleteMessages(sqsClient, createResult.queueUrl(),  messages);
                sendMessage(sqsClient, createResult.queueUrl(), "despacho");
            } else if (from.equals("despacho")){
                System.out.println("APROVACAO , enviando cliente");
                deleteMessages(sqsClient, createResult.queueUrl(),  messages);
                sendMessage(sqsClient, createResult.queueUrl(), "pedidoFim");
            }else if (from.equals("pedidoFim")){
                System.out.println("ENTREGUE");
                deleteMessages(sqsClient, createResult.queueUrl(),  messages);                
            }

        }        

        sqsClient.close();
    }

    public static  List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .waitTimeSeconds(20)
            .maxNumberOfMessages(5)
            .build();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        return messages;
    }

    public static void deleteMessages(SqsClient sqsClient, String queueUrl,  List<Message> messages) {
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        }
   }

   public static void sendMessage(SqsClient sqsClient, String queueUrl, String message) {
    SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
        .queueUrl(queueUrl)       
        .messageBody(message)
        .build();
    sqsClient.sendMessage(sendMsgRequest);
}
}
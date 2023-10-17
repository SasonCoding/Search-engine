package com.handson.searchengine.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.searchengine.crawler.Crawler;

import com.handson.searchengine.model.CrawlStatusOut;
import com.handson.searchengine.model.CrawlerRecord;
import com.handson.searchengine.model.CrawlerRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Random;

@RestController
@RequestMapping("/api")
public class AppController {

    private static final int ID_LENGTH = 6;
    private Random random = new Random();

    @Autowired
    ObjectMapper om;

    @Value("${cloud.aws.end-point.uri}")
    private String endPoint;

    @Autowired
    Crawler crawler;

    @Autowired
    QueueMessagingTemplate queueMessagingTemplate;

    @RequestMapping(value = "/crawl", method = RequestMethod.POST)
    public String crawl(@RequestBody CrawlerRequest request) throws IOException, InterruptedException {
        String crawlId = generateCrawlId();
        if (!request.getUrl().startsWith("http")) {
            request.setUrl("https://" + request.getUrl());
        }
        new Thread(()-> {
            try {
                crawler.crawl(crawlId, request);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        return crawlId;
    }

    @RequestMapping(value = "/crawl/{crawlId}", method = RequestMethod.GET)
    public CrawlStatusOut getCrawl(@PathVariable String crawlId) throws IOException, InterruptedException {
        return crawler.getCrawlInfo(crawlId);
    }

    private String generateCrawlId() {
        String charPool = "ABCDEFHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < ID_LENGTH; i++) {
            res.append(charPool.charAt(random.nextInt(charPool.length())));
        }
        return res.toString();
    }


    @RequestMapping(value = "/sendSQS", method = RequestMethod.POST)
    public String sendSQS(@RequestBody String message) {
        queueMessagingTemplate.send(endPoint, MessageBuilder.withPayload(message).build());
        return "OK";
    }
    @SqsListener("sqs-queue")
    public void loadMessagesFromQueue(String message) throws IOException, InterruptedException {
        System.out.println("Queue message: " + message);
            CrawlerRecord rec = om.readValue(message, CrawlerRecord.class);
            crawler.crawlOneRecord(rec.getCrawlId(), rec);
    }
}

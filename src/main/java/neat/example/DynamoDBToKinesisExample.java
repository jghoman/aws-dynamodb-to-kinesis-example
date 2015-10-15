/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package neat.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Quick example of populating a DynamoDB and using the Kinesis streams
 * adapter to consume from it.
 *
 * Start a producer thread that writes records to DynamoDB and a consumer
 * thread that starts a Kinesis worker that just prints those records.
 *
 * Quick, dirty, simple.
 */
public class DynamoDBToKinesisExample {

  private static AWSCredentialsProvider provider = new ProfileCredentialsProvider("personal");
  private static AmazonDynamoDBClient dynamoDBClient =
      new AmazonDynamoDBClient(provider);

  public static void main(String[] args) throws InterruptedException {
    String tableName = "my-first-table";

    if(Tables.doesTableExist(dynamoDBClient, tableName)) {
      String status = dynamoDBClient.describeTable(new DescribeTableRequest(tableName)).getTable().getTableStatus();
      System.out.println("Status = " + status);
      System.out.println("Table exists.  Deleting and re-creating");
      System.out.println(dynamoDBClient.deleteTable(tableName));

      long sleep_ms = 500l;

      while(true) {
        try {
          status = dynamoDBClient.describeTable(new DescribeTableRequest(tableName)).getTable().getTableStatus();
        } catch(ResourceNotFoundException rnfe) {
          System.out.println("Table disappeared. Breaking.");
          break;
        }
        System.out.println("Status = " + status);
        if(!status.equals("DELETING")) break;

        System.out.println("Sleeping for " + sleep_ms + " ms");
        Thread.sleep(sleep_ms);
      }
    }

    createTable(tableName);

    DescribeTableResult describeTableResult = dynamoDBClient.describeTable(tableName);

    final String myStreamArn = describeTableResult.getTable().getLatestStreamArn();
    StreamSpecification myStreamSpec =
        describeTableResult.getTable().getStreamSpecification();

    System.out.println("Current stream ARN for " + tableName + ": "+ myStreamArn);
    System.out.println("Stream enabled: "+ myStreamSpec.getStreamEnabled());
    System.out.println("Update view type: "+ myStreamSpec.getStreamViewType());

    final CountDownLatch start = new CountDownLatch(1);

    // Start producer
    Runnable runnable = () -> {
      try {
        Random random = new Random();
        start.await();

        int numChanges = 0;
        System.out.println("Making some changes to table data");
        for(int i = 1; i < 14; i++) {
          putItem(tableName, i);
          numChanges++;
          // A bit of a delay during production
          Thread.sleep(random.nextInt(4) * 1000);


        }
        System.out.println("Total items put: " + numChanges);

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };

    Thread producer = new Thread(runnable);
    producer.start();

    AmazonKinesis adapterClient = new AmazonDynamoDBStreamsAdapterClient(provider);

    AmazonCloudWatchClient cloudWatchClient = new AmazonCloudWatchClient(provider);

    KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("testing-dynamo-db-streams", myStreamArn, provider, "John T. Worker")
        .withMaxRecords(1000)
        .withMetricsLevel(MetricsLevel.NONE)
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    Worker worker = new Worker(new CoolStreamsRecordProcessorFactory(), workerConfig, adapterClient, dynamoDBClient, cloudWatchClient);

    Thread t2 = new Thread(worker);
    t2.start();

    System.out.println("Triggering producer and worker");
    start.countDown();
    producer.join();

    System.out.println("Producer done, shutting down consumer.");
    worker.shutdown();

    System.out.println("Shutting down Kinesis");
    adapterClient.shutdown();

    System.out.println("Shutting down dynamoDBClient");
    dynamoDBClient.shutdown();

    System.out.println("Shutting down cloudwatch client");
    cloudWatchClient.shutdown();

    System.out.println("-30-");
  }

  private static void putItem(String tableName, int i) {
    Map<String, AttributeValue> item = new HashMap<>(1);
    item.put("Id", new AttributeValue().withN(Integer.toString(i)));
    item.put("Message", new AttributeValue().withS("Item: " + i));
    dynamoDBClient.putItem(tableName, item);
    System.out.println("Put: " + item);
  }

  private static void createTable(String tableName) throws InterruptedException {
    ArrayList<AttributeDefinition> attributeDefinitions =
        new ArrayList<>();

    attributeDefinitions.add(new AttributeDefinition()
        .withAttributeName("Id")
        .withAttributeType("N"));

    ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement()
        .withAttributeName("Id")
        .withKeyType(KeyType.HASH));

    StreamSpecification streamSpecification = new StreamSpecification();
    streamSpecification.setStreamEnabled(true);
    streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);

    CreateTableRequest createTableRequest = new CreateTableRequest()
        .withTableName(tableName)
        .withKeySchema(keySchema)
        .withAttributeDefinitions(attributeDefinitions)
        .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L))
        .withStreamSpecification(streamSpecification);

    dynamoDBClient.createTable(createTableRequest);
    Tables.awaitTableToBecomeActive(dynamoDBClient, tableName);
  }
}

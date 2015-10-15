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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.nio.charset.Charset;
import java.util.List;

public class CoolStreamProcessor implements IRecordProcessor {

  private String shardId;

  @Override
  public void initialize(String shardId) {
    this.shardId = shardId;
    System.out.println("Initializing " + shardId);
  }

  @Override
  public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
    System.out.println(shardId + " processing " + records.size() + " records");
    for(Record record : records) {
      String data = new String(record.getData().array(), Charset.forName("UTF-8"));
      System.out.printf("%s: partition key = %s data =", shardId, record.getPartitionKey(), data);
    }
  }

  @Override
  public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
    System.out.println("Shutting down because: " + reason);
  }
}
